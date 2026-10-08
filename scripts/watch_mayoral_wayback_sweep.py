"""Slack watcher for a Wayback domain-pattern sweep (job-alerts format).

Posts once at start, every 6 hours while running, once on a 30-minute stall
(then again only after activity returns and stalls again), and once at the end.
Run on the host of the sweep, from the sweep's working directory, for example:
  SWEEP_DIR=data/municipal_discovery/top100_coverage/url_discovery/sweep_after_20261007 \
    nohup python3 scripts/watch_mayoral_wayback_sweep.py > $SWEEP_DIR/watch.log 2>&1 &
The sweep runs under a wrapper that logs "sweep finished rc=<n>" to wayback.log.
Set SWEEP_LABEL for the Task line and HOST_LABEL for the host. WATCH_DRY_RUN=1 prints instead of posting.
"""
from __future__ import annotations

import csv
import json
import os
import re
import subprocess
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

SWEEP = Path(os.environ["SWEEP_DIR"])
HITS, LOG = SWEEP / "wayback_hits.jsonl", SWEEP / "wayback.log"
HELPER = Path("scripts/slack_status.sh")
PROJECT = "Candidate websites, mayoral extension"
TASK = "Wayback search for missing mayoral campaign sites, " + os.environ.get("SWEEP_LABEL", SWEEP.name)
WHERE = f"log {SWEEP.name}/wayback.log"
PACIFIC = ZoneInfo("America/Los_Angeles")
POLL, PROGRESS_EVERY, STALL_AFTER = 300, 6 * 3600, 1800


def running() -> bool:
    return subprocess.run(["pgrep", "-f", f"sweep_mayoral_domain_patterns.py wayback --sweep-dir {SWEEP}"],
                          capture_output=True).returncode == 0


def counts() -> tuple[int, int, int]:
    """Domain windows queried, planned, and with captures (latest record per key)."""
    planned = len({f"{r['domain']}|{r['window_start']}|{r['window_end']}" for r in csv.DictReader((SWEEP / "patterns.csv").open())})
    latest = {}
    if HITS.exists():
        for line in HITS.open():
            if line.strip():
                record = json.loads(line)
                latest[record["key"]] = record
    done = sum(1 for r in latest.values() if r["status"] == "ok")
    found = sum(1 for r in latest.values() if r["captures"])
    return done, planned, found


def when(moment: datetime) -> str:
    return moment.astimezone(PACIFIC).strftime("%a %d %b %H:%M %Z")


def duration(seconds: float) -> str:
    hours = seconds / 3600
    return f"{seconds / 60:.0f} minutes" if hours < 1 else f"{hours:.1f} hours" if hours < 48 else f"{hours / 24:.1f} days"


def post(level: str, status: str, eta: str) -> None:
    if os.environ.get("WATCH_DRY_RUN"):
        os.environ["SLACK_DRY_RUN"] = "1"
    subprocess.run(["bash", "-c", '. "$0"; slack_status "$@"', str(HELPER), level, PROJECT, TASK, status, eta, WHERE],
                   env=dict(os.environ, SLACK_REPO="hhilbig/candidate_website_extension",
                            HOST_LABEL=os.environ.get("HOST_LABEL", "droplet")))


def main() -> None:
    started = time.time()
    done, planned, found = counts()
    samples = [(started, done)]
    last_progress, stalled = started, False
    post("FYI", f"Watcher started. {done:,} of {planned:,} domain searches done.", "Estimate follows in the next update.")
    while True:
        time.sleep(POLL)
        done, planned, found = counts()
        now = time.time()
        samples = [(t, n) for t, n in samples if now - t <= 3600] + [(now, done)]
        if not running():
            text = LOG.read_text() if LOG.exists() else ""
            if re.search(r"sweep finished rc=0", text) and done == planned:
                post("FYI", f"Finished. {found} domains have archived pages after the election, before screening.",
                     "Done. Next is the page screen and an agent check.")
            else:
                last = text.strip().splitlines()[-1][:160] if text.strip() else "empty log"
                post("ACTION NEEDED", f"Stopped with {done:,} of {planned:,} searches done. Last log line: {last}",
                     "Unknown until it is restarted.")
            return
        active = HITS.exists() and now - HITS.stat().st_mtime < STALL_AFTER
        if not active and not stalled:
            post("ACTION NEEDED", "Stalled. No new results for 30 minutes. The archive may be refusing requests.",
                 "Unknown until it resumes.")
            stalled = True
        elif active and stalled:
            stalled = False
        if now - last_progress >= PROGRESS_EVERY and active:
            rate = (samples[-1][1] - samples[0][1]) / max(1, samples[-1][0] - samples[0][0])
            eta = (f"{when(datetime.now(timezone.utc) + timedelta(seconds=max(0, planned - done) / rate))}. The archive is slow at times, so this can slip."
                   if rate > 0 else "Unknown while the archive returns errors.")
            post("FYI", f"Running for {duration(now - started)}. {done:,} of {planned:,} domain searches done.", eta)
            last_progress = now


if __name__ == "__main__":
    main()
