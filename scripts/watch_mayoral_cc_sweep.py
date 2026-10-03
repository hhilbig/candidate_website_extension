"""Slack watcher for the Common Crawl part of the mayoral domain sweep (job-alerts format).

Posts once at start, every 6 hours while running, once on a 30-minute stall
(then again only after activity returns and stalls again), and once at the end.
Run on the host of the sweep, from the sweep's working directory:
  nohup python3 scripts/watch_mayoral_cc_sweep.py > <sweep>/watch.log 2>&1 &
Set RUN_BASELINE to the record count when the current pass began (751 for the October 3 pass).
Set WATCH_DRY_RUN=1 to print messages instead of posting.
"""
from __future__ import annotations

import json
import os
import re
import subprocess
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

SWEEP = Path("data/municipal_discovery/top100_coverage/url_discovery/sweep_20261002")
HITS, LOG = SWEEP / "commoncrawl_hits.jsonl", SWEEP / "commoncrawl.log"
HELPER = Path("scripts/slack_status.sh")
PROJECT = "Candidate websites, mayoral extension"
TASK = "Common Crawl search for missing mayoral campaign sites"
WHERE = "log sweep_20261002/commoncrawl.log"
PACIFIC = ZoneInfo("America/Los_Angeles")
POLL, PROGRESS_EVERY, STALL_AFTER = 300, 6 * 3600, 1800


def sweep_running() -> bool:
    return subprocess.run(["pgrep", "-f", "sweep_mayoral_domain_patterns.py commoncrawl"],
                          capture_output=True).returncode == 0


def counts() -> tuple[int, int, int]:
    """Queries done in the current run, queries planned, and domain windows with captures overall."""
    planned = [int(m) for m in re.findall(r"commoncrawl: (\d+) domain-crawl queries", LOG.read_text())]
    records = [json.loads(line) for line in HITS.open() if line.strip()]
    found = len({"|".join(r["key"].split("|")[:3]) for r in records if r["captures"]})
    return len(records), planned[-1] if planned else 0, found


def when(moment: datetime) -> str:
    return moment.astimezone(PACIFIC).strftime("%a %d %b %H:%M %Z")


def duration(seconds: float) -> str:
    hours = seconds / 3600
    return f"{seconds / 60:.0f} minutes" if hours < 1 else f"{hours:.1f} hours" if hours < 48 else f"{hours / 24:.1f} days"


def post(level: str, status: str, eta: str) -> None:
    if os.environ.get("WATCH_DRY_RUN"):
        os.environ["SLACK_DRY_RUN"] = "1"
    subprocess.run(["bash", "-c", '. "$0"; slack_status "$@"', str(HELPER), level, PROJECT, TASK, status, eta, WHERE],
                   env=dict(os.environ, SLACK_REPO="hhilbig/candidate_website_extension", HOST_LABEL="droplet"))


def main() -> None:
    started = datetime.now(timezone.utc)
    # Records written before the current pass began (the pass size in the log excludes them).
    base_records = int(os.environ.get("RUN_BASELINE", "0"))
    _, planned, _ = counts()
    samples = [(time.time(), counts()[0])]
    last_progress, stalled = time.time(), False
    post("FYI", f"Watcher started. The search is running, with {planned:,} queries in the current pass.",
         "Estimate follows in the next update.")
    while True:
        time.sleep(POLL)
        records, planned, found = counts()
        now = time.time()
        samples = [(t, n) for t, n in samples if now - t <= 3600] + [(now, records)]
        if not sweep_running():
            if "commoncrawl done" in LOG.read_text():
                post("FYI", f"Finished. {found} domains have an archived page in Common Crawl so far, before screening.",
                     "Done. Next is the page screen and an agent check.")
            else:
                last = LOG.read_text().strip().splitlines()[-1][:160] if LOG.read_text().strip() else "empty log"
                post("ACTION NEEDED", f"Failed. The search stopped without finishing. Last log line: {last}",
                     "Unknown until it is restarted.")
            return
        active = now - HITS.stat().st_mtime < STALL_AFTER
        if not active and not stalled:
            post("ACTION NEEDED", "Stalled. No new results for 30 minutes. Common Crawl may be refusing requests.",
                 "Unknown until it resumes.")
            stalled = True
        elif active and stalled:
            stalled = False
        if now - last_progress >= PROGRESS_EVERY and active:
            done_now = max(0, records - base_records)
            rate = (samples[-1][1] - samples[0][1]) / max(1, samples[-1][0] - samples[0][0])
            left = max(0, planned - done_now)
            eta = (f"{when(datetime.now(timezone.utc) + timedelta(seconds=left / rate))}. Common Crawl throttles often, so this can slip."
                   if rate > 0 else "Unknown while Common Crawl is throttling.")
            post("FYI", f"Running for {duration(now - started.timestamp())}. {done_now:,} of {planned:,} queries done. "
                        f"{found} domains with archived pages so far.", eta)
            last_progress = now


if __name__ == "__main__":
    main()
