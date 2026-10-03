"""Summarize saved stage 3 pages per finalist for the coverage build.

Reads a finished stage 3 run (scripts/scrape_mayoral_wayback_batch.py) and its
frozen targets, re-hashes every saved raw and text file, and writes one row per
target: pages saved with text on the campaign domain, and how many fall within
365 days of the election (the build's period rule). No archive requests.

Run where the run directory lives (mac2), then copy the CSV and manifest into
data/municipal_discovery/top100_coverage/inputs/.
"""
from __future__ import annotations

import argparse
import csv
import gzip
import hashlib
import json
from datetime import date, datetime
from pathlib import Path

FIELDS = ["candidate_election_id", "target_id", "registrable_domain", "election_date", "pages_fetched",
          "pages_with_text", "pages_with_text_in_window", "text_characters_in_window", "first_timestamp_in_window",
          "last_timestamp_in_window", "files_verified"]


def sha(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def content_sha(path: Path) -> str:
    """The runner hashes the decoded response; raw pages are stored gzip-compressed."""
    data = path.read_bytes()
    return hashlib.sha256(gzip.decompress(data) if path.suffix == ".gz" else data).hexdigest()


def in_window(timestamp: str, election: str) -> bool:
    captured = datetime.strptime(timestamp[:8], "%Y%m%d").date()
    return abs((captured - date.fromisoformat(election)).days) <= 365


def summarize(targets: list[dict], attempts: list[dict], run_dir: Path, verify=True) -> list[dict]:
    by_target = {}
    for row in attempts:
        by_target.setdefault(row["target_id"], []).append(row)
    out = []
    for target in targets:
        election = target["election_date"]
        fetched = [r for r in by_target.get(target["target_id"], []) if r["fetch_status"] == "fetched"]
        with_text = [r for r in fetched if int(r["text_characters"] or 0) > 0]
        window = [r for r in with_text if in_window(r["effective_timestamp"] or r["timestamp"], election)]
        verified = all((run_dir / r[p]).exists() and (not verify or content_sha(run_dir / r[p]) == r[h])
                       for r in window for p, h in (("raw_html_path", "raw_html_sha256"), ("text_path", "text_sha256")))
        stamps = sorted(r["effective_timestamp"] or r["timestamp"] for r in window)
        out.append({"candidate_election_id": target["candidate_contest_id"], "target_id": target["target_id"],
                    "registrable_domain": target["registrable_domain"], "election_date": election,
                    "pages_fetched": len(fetched), "pages_with_text": len(with_text),
                    "pages_with_text_in_window": len(window),
                    "text_characters_in_window": sum(int(r["text_characters"]) for r in window),
                    "first_timestamp_in_window": stamps[0] if stamps else "",
                    "last_timestamp_in_window": stamps[-1] if stamps else "", "files_verified": verified})
    return out


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--targets", type=Path, required=True, help="candidate_site_targets.csv (has election_date)")
    parser.add_argument("--run-dir", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    targets = list(csv.DictReader(args.targets.open()))
    attempts_path = args.run_dir / "fetch_attempts.csv"
    rows = summarize(targets, list(csv.DictReader(attempts_path.open())), args.run_dir)
    with args.output.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=FIELDS)
        writer.writeheader()
        writer.writerows(rows)
    manifest = {"fetch_attempts_sha256": sha(attempts_path), "targets_sha256": sha(args.targets),
                "output_sha256": sha(args.output), "rows": len(rows),
                "rows_with_pages_in_window": sum(r["pages_with_text_in_window"] > 0 for r in rows),
                "all_files_verified": all(r["files_verified"] for r in rows)}
    args.output.with_suffix(".manifest.json").write_text(json.dumps(manifest, indent=1) + "\n")
    print(json.dumps(manifest, indent=1))


if __name__ == "__main__":
    main()
