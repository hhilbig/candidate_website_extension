"""Merge the Hive page-check label files into the table the corpus build reads (page_id, label).

Label files are given oldest first; a later file replaces the label of a page that an earlier file labeled (a relabel
with a newer prompt). Records with a parse error carry no label, so the page keeps no page-check label.
Year rule (prompt v3, October 17, 2026): an earlier_campaign page whose race_year is the election year or the year
before becomes this_campaign, because an earlier round of the same race falls in that period. Election pages for a
race two or more years earlier stay earlier_campaign.

Usage:
  python scripts/build_mayoral_page_check_table.py --items <items.jsonl ...> --labels <labels.jsonl ...> --output <csv>
"""
from __future__ import annotations

import argparse
import csv
import json
from collections import Counter
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--items", type=Path, nargs="+", required=True, help="Hive item files (for the election year)")
    parser.add_argument("--labels", type=Path, nargs="+", required=True, help="label files, oldest first")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    election_year = {}
    for path in args.items:
        for line in path.open():
            item = json.loads(line)
            election_year[item["item_id"]] = int(item["election_date"][:4])
    final = {}
    for path in args.labels:
        for line in path.open():
            record = json.loads(line)
            if record.get("status") == "ok":
                final[record["item_id"]] = record
    counts, rule = Counter(), 0
    with args.output.open("w", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["page_id", "label", "prompt_version", "race_year"])
        for page_id, record in sorted(final.items()):
            label, year = record["label"], record.get("race_year")
            if label == "earlier_campaign" and isinstance(year, int) and 0 <= election_year[page_id] - year <= 1:
                label, rule = "this_campaign", rule + 1
            counts[label] += 1
            writer.writerow([page_id, label, record["prompt_version"], "" if year is None else year])
    print(json.dumps({"pages": len(final), "labels": dict(counts), "kept_by_year_rule": rule}, indent=1))


if __name__ == "__main__":
    main()
