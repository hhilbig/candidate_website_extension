"""Slack watcher for one mayoral stage 3 Wayback scrape batch (job-alerts format).

Posts once at start, every 6 hours while running, once on a 30-minute stall
(then again only after activity returns and stalls again), and once at the end.
Run on mac2 from ~/candidate_website_extension_stage3, for example:
  BATCH=data/municipal_archive_acquisition/mayoral_stage3r_20261004 \
    nohup caffeinate -i python3 scripts/watch_mayoral_stage3_scrape.py > $BATCH/watch.log 2>&1 &
Set BATCH_LABEL to a short name for the Task line. Set WATCH_DRY_RUN=1 to print messages instead of posting.
"""
from __future__ import annotations

import csv
import json
import os
import subprocess
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

BATCH = Path(os.environ["BATCH"])
RUN = BATCH / "runs" / "production"
LOG = BATCH / "runs.log"
HELPER = Path("scripts/slack_status.sh")
PROJECT = "Candidate websites, mayoral extension"
TASK = "Wayback page download, " + os.environ.get("BATCH_LABEL", BATCH.name)
WHERE = f"log {BATCH.name}/runs.log"
PACIFIC = ZoneInfo("America/Los_Angeles")
POLL, PROGRESS_EVERY, STALL_AFTER = 300, 6 * 3600, 1800
DONE = {"fetched", "fetched_timestamp_mismatch", "no_content", "not_available", "non_html", "redirect_loop"}


def running() -> bool:
    return subprocess.run(["pgrep", "-f", f"{BATCH}/frozen/targets.csv"], capture_output=True).returncode == 0


def counts() -> tuple[int, int, int]:
    """Pages with a final result, pages with text, and pages in the fetch queue."""
    latest: dict[str, str] = {}
    for name in ("fetch_attempts.csv", "fetch_attempts.journal.csv"):
        path = RUN / name
        if path.exists():
            with path.open(newline="") as handle:
                for row in csv.DictReader(handle):
                    latest[row["capture_id"]] = row["fetch_status"]
    queue = RUN / "fetch_queue.csv"
    planned = sum(1 for _ in queue.open()) - 1 if queue.exists() else 0
    done = sum(1 for s in latest.values() if s in DONE)
    fetched = sum(1 for s in latest.values() if s.startswith("fetched"))
    return done, fetched, planned


def last_activity() -> float:
    paths = [LOG, RUN / "fetch_attempts.csv", RUN / "fetch_attempts.journal.csv"]
    return max((p.stat().st_mtime for p in paths if p.exists()), default=0.0)


def when(moment: datetime) -> str:
    return moment.astimezone(PACIFIC).strftime("%a %d %b %H:%M %Z")


def duration(seconds: float) -> str:
    hours = seconds / 3600
    return f"{seconds / 60:.0f} minutes" if hours < 1 else f"{hours:.1f} hours" if hours < 48 else f"{hours / 24:.1f} days"


def post(level: str, status: str, eta: str) -> None:
    if os.environ.get("WATCH_DRY_RUN"):
        os.environ["SLACK_DRY_RUN"] = "1"
    subprocess.run(["bash", "-c", '. "$0"; slack_status "$@"', str(HELPER), level, PROJECT, TASK, status, eta, WHERE],
                   env=dict(os.environ, SLACK_REPO="hhilbig/candidate_website_extension", HOST_LABEL="mac2"))


def main() -> None:
    started = time.time()
    done, _, planned = counts()
    samples = [(started, done)]
    last_progress, stalled = started, False
    post("FYI", f"Watcher started. {done:,} of {planned:,} pages have a final result.",
         "Estimate follows in the next update.")
    while True:
        time.sleep(POLL)
        done, fetched, planned = counts()
        now = time.time()
        samples = [(t, n) for t, n in samples if now - t <= 3600] + [(now, done)]
        if not running():
            audit_path = RUN / "fetch_audit.json"
            audit = json.loads(audit_path.read_text()) if audit_path.exists() else {}
            if audit.get("stage") == "fetch_complete":
                post("FYI", f"Finished after {duration(now - started)} of watching. {fetched:,} pages saved, "
                            f"{audit.get('retryable_rows', 0)} left retryable.",
                     "Done. Next is the page summary and the coverage rebuild.")
            else:
                text = LOG.read_text().strip() if LOG.exists() else ""
                last = text.splitlines()[-1][:160] if text else "empty log"
                post("ACTION NEEDED", f"Failed. The download stopped without finishing. Last log line: {last}",
                     "Unknown until it is restarted.")
            return
        active = now - last_activity() < STALL_AFTER
        if not active and not stalled:
            post("ACTION NEEDED", "Stalled. No log or result activity for 30 minutes. The archive may be refusing requests.",
                 "Unknown until it resumes.")
            stalled = True
        elif active and stalled:
            stalled = False
        if now - last_progress >= PROGRESS_EVERY and active:
            rate = (samples[-1][1] - samples[0][1]) / max(1, samples[-1][0] - samples[0][0])
            left = max(0, planned - done)
            eta = (f"{when(datetime.now(timezone.utc) + timedelta(seconds=left / rate))}. The archive is slow at times, so this can slip."
                   if rate > 0 else "Unknown while the archive returns errors.")
            post("FYI", f"Running for {duration(now - started)}. {done:,} of {planned:,} pages have a final result.", eta)
            last_progress = now


if __name__ == "__main__":
    main()
