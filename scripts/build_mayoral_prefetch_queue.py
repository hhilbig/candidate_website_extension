"""Build fetch queues for pre-election copies of mayoral campaign pages.

The site scrape downloaded one capture per archived page, the one nearest the election. For many
pages that capture is from election day or later, although the Wayback Machine also holds an earlier
copy. Hanno decided on October 16, 2026 to fetch those earlier copies, so the release can give the
text that was online before the vote.

For every page in the release corpus with a capture on or after election day (and not excluded as a
listing page, whose earlier copy is a listing too), this script looks up the same target and page in
the batch's capture inventory and selects the latest capture in the 365 days before the election.
Election dates come from the corpus (the finalist's election date), not from the batch target files.
No new CDX query is made. The output is one run folder per batch, runs/prefetch/fetch_queue.csv,
which `scrape_mayoral_wayback_batch.py fetch --run-dir` reads.

Run on mac2 from ~/candidate_website_extension_stage3:
  .venv/bin/python scripts/build_mayoral_prefetch_queue.py --corpus <mayoral_page_corpus.parquet> \
      --target-map <target_to_finalist.json> [--dry-run]
"""
from __future__ import annotations

import argparse
import csv
import json
import sys
from collections import Counter
from datetime import date, datetime, timedelta
from pathlib import Path

import pyarrow.parquet as pq

csv.field_size_limit(sys.maxsize)
ACQ = Path("data/municipal_archive_acquisition")
WINDOW_DAYS = 365


def page_key(url: str) -> str:
    """Address without scheme, "www.", port 80, fragment and trailing slash, lower case."""
    u = url.strip().lower()
    for prefix in ("https://", "http://"):
        if u.startswith(prefix):
            u = u[len(prefix):]
    u = u.removeprefix("www.").split("#")[0]
    host, _, rest = u.partition("/")
    host = host.removesuffix(":80")
    return (host + "/" + rest).rstrip("/")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--corpus", type=Path, required=True)
    parser.add_argument("--target-map", type=Path, required=True)
    parser.add_argument("--run-name", default="prefetch")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    corpus = pq.read_table(args.corpus, columns=["target_id", "original_url", "capture_timestamp",
                                                 "days_from_election", "exclusion_reason"]).to_pylist()
    election = {}
    wanted: dict[str, set[str]] = {}
    for row in corpus:
        stamp = datetime.strptime(row["capture_timestamp"][:8], "%Y%m%d").date()
        election.setdefault(row["target_id"], stamp - timedelta(days=row["days_from_election"]))
        if row["days_from_election"] >= 0 and "listing_page" not in row["exclusion_reason"]:
            wanted.setdefault(row["target_id"], set()).add(page_key(row["original_url"]))
    batches = {t: b for b, targets in json.loads(args.target_map.read_text()).items() for t in targets}
    audit = {"pages_wanted": sum(map(len, wanted.values())), "batches": {}}
    for batch in sorted({batches[t] for t in wanted if t in batches}):
        source = ACQ / batch / "runs" / "production"
        with (source / "fetch_queue.csv").open(newline="") as handle:
            columns = csv.DictReader(handle).fieldnames
        best: dict[tuple[str, str], dict] = {}
        counts: Counter = Counter()
        with (source / "capture_inventory.csv").open(newline="") as handle:
            for row in csv.DictReader(handle):
                target = row["target_id"]
                key = page_key(row["original_url"])
                if key not in wanted.get(target, ()) or row.get("statuscode", "200") != "200":
                    continue
                days = (datetime.strptime(row["timestamp"][:8], "%Y%m%d").date() - election[target]).days
                if not -WINDOW_DAYS <= days < 0:
                    counts["capture_outside_pre_window"] += 1
                    continue
                if (target, key) not in best or row["timestamp"] > best[(target, key)]["timestamp"]:
                    best[(target, key)] = {**row, "capture_days_signed": days, "capture_days_absolute": -days,
                                           "election_reference_date": election[target].isoformat()}
        rows = [{c: r.get(c, "") for c in columns} | {"selection_reason": "latest_pre_election_capture_per_page",
                                                     "max_pages_per_target": ""}
                for r in sorted(best.values(), key=lambda r: (r["target_id"], r["timestamp"]))]
        audit["batches"][batch] = {"pages_wanted": sum(len(wanted[t]) for t in wanted if batches.get(t) == batch),
                                   "pre_election_copies": len(rows), **counts}
        if not args.dry_run and rows:
            out = ACQ / batch / "runs" / args.run_name
            out.mkdir(parents=True, exist_ok=True)
            with (out / "fetch_queue.csv").open("w", newline="") as handle:
                writer = csv.DictWriter(handle, fieldnames=columns)
                writer.writeheader()
                writer.writerows(rows)
    audit["pre_election_copies"] = sum(b["pre_election_copies"] for b in audit["batches"].values())
    print(json.dumps(audit, indent=1))
    if not args.dry_run:
        (Path(args.corpus).parent / "prefetch_queue_audit.json").write_text(json.dumps(audit, indent=1))


if __name__ == "__main__":
    main()
