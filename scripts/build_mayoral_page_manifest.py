"""List every saved page of the campaign sites credited to mayoral finalists, across all scrape runs.

One row per fetched page: finalist, target, batch, run folder, archived URL and time, text file and hash,
and whether the capture falls within 365 days of the election (the coverage build's text rule).
Sources are every batch that credits text to a finalist (the stage 3 batches and the August 26 batches)
and every run folder of a batch: the capped run (production), the uncapped run (uncapped) and supplements
(carymoon_extra). Only targets in the target-to-finalist map are listed, so non-campaign targets are left out.

Run on mac2 from ~/candidate_website_extension_stage3:
  python3 scripts/build_mayoral_page_manifest.py --map target_to_finalist.json --output mayoral_page_manifest.csv.gz
It also writes <output stem>_by_finalist.csv with pages per finalist.
"""
from __future__ import annotations

import argparse
import csv
import gzip
import json
import sys
from collections import defaultdict
from datetime import date, datetime
from pathlib import Path

csv.field_size_limit(sys.maxsize)
ACQ = Path("data/municipal_archive_acquisition")
RUNS = ("production", "uncapped", "carymoon_extra")
FETCHED = {"fetched"}
FIELDS = ["candidate_election_id", "target_id", "capture_id", "batch", "run", "registrable_domain", "original_url",
          "timestamp", "effective_replay_url", "effective_timestamp", "days_from_election", "in_text_window", "text_path", "text_sha256", "text_characters",
          "raw_html_path", "raw_html_sha256"]


def election_dates(batch: Path) -> dict[str, str]:
    path = batch / "candidate_site_targets.csv"
    if not path.exists():
        path = batch / "frozen" / "targets.csv"
    out = {}
    for row in csv.DictReader(path.open()):
        out[row["target_id"]] = row.get("election_date") or row.get("election_reference_date", "")
    return out


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--map", type=Path, required=True, help="JSON {batch or _stage3: {target_id: candidate_election_id}}")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    mapping = json.loads(args.map.read_text())
    stage3 = mapping.pop("_stage3", {})
    batches = sorted(ACQ.glob("mayoral_stage3*")) + [ACQ / b for b in mapping]
    per_finalist = defaultdict(lambda: {"pages": 0, "pages_in_window": 0, "chars_in_window": 0, "pages_capped_run": 0,
                                        "sites": set()})
    seen, rows_out = set(), 0
    with gzip.open(args.output, "wt", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=FIELDS)
        writer.writeheader()
        for batch in batches:
            credited = mapping.get(batch.name, stage3)
            dates = election_dates(batch)
            for run in RUNS:
                attempts = batch / "runs" / run / "fetch_attempts.csv"
                if not attempts.exists():
                    continue
                for row in csv.DictReader(attempts.open()):
                    finalist = credited.get(row["target_id"])
                    if not finalist or row["fetch_status"] not in FETCHED or int(row["text_characters"] or 0) == 0:
                        continue
                    key = (batch.name, row["capture_id"])
                    if key in seen:
                        raise ValueError(f"capture listed twice: {key}")
                    seen.add(key)
                    stamp = row["effective_timestamp"] or row["timestamp"]
                    days = (datetime.strptime(stamp[:8], "%Y%m%d").date() - date.fromisoformat(dates[row["target_id"]])).days
                    window = abs(days) <= 365
                    writer.writerow({"candidate_election_id": finalist, "target_id": row["target_id"],
                                     "capture_id": row["capture_id"], "batch": batch.name,
                                     "run": run, "registrable_domain": row.get("registrable_domain", ""),
                                     "original_url": row["original_url"], "timestamp": row["timestamp"],
                                     "effective_replay_url": row["effective_replay_url"],
                                     "effective_timestamp": row["effective_timestamp"], "days_from_election": days,
                                     "in_text_window": window, "text_path": f"{batch}/runs/{run}/{row['text_path']}",
                                     "text_sha256": row["text_sha256"], "text_characters": row["text_characters"],
                                     "raw_html_path": f"{batch}/runs/{run}/{row['raw_html_path']}",
                                     "raw_html_sha256": row["raw_html_sha256"]})
                    rows_out += 1
                    stats = per_finalist[finalist]
                    stats["pages"] += 1
                    stats["pages_capped_run"] += run == "production"
                    stats["pages_in_window"] += window
                    stats["chars_in_window"] += int(row["text_characters"]) if window else 0
                    stats["sites"].add(row["target_id"])
    summary = args.output.with_name(args.output.name.split(".")[0] + "_by_finalist.csv")
    with summary.open("w", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["candidate_election_id", "sites", "pages", "pages_capped_run", "pages_in_window", "chars_in_window"])
        for finalist, s in sorted(per_finalist.items()):
            writer.writerow([finalist, len(s["sites"]), s["pages"], s["pages_capped_run"], s["pages_in_window"], s["chars_in_window"]])
    print(json.dumps({"pages": rows_out, "finalists": len(per_finalist),
                      "finalists_with_pages_in_window": sum(1 for s in per_finalist.values() if s["pages_in_window"])}))


if __name__ == "__main__":
    main()
