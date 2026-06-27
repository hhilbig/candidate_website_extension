#!/usr/bin/env bash
# Autonomous Senate scrape queue (runs on the droplet).
#
# Takes an ordered list of roster CSV paths and runs the Wayback scraper on
# each sequentially at threads=1, with per-roster logs and Slack pings. The
# scraper derives its progress file from the roster stem, so:
#   - roster_senate_2016.csv  -> resumes from progress_roster_senate_2016.csv
#   - roster_senate_2018.csv  -> fresh (no progress yet)
#   - roster_senate_YYYY_recovery.csv -> fresh progress, recovers CDX failures
#
# Compression / tar swaps are deliberately NOT done here — those are
# supervised checkpoints (Mar-12 data-loss rule). This script only scrapes.
#
# Usage:
#   nohup scripts/run_senate_queue.sh \
#       data/rosters/roster_senate_2016.csv \
#       data/rosters/roster_senate_2018.csv \
#       data/rosters/roster_senate_2020_recovery.csv ... &
set -u
cd ~/candidate_website_extension
source .venv/bin/activate
set -a; source .env 2>/dev/null; set +a
HOST=$(hostname)
QLOG=logs/senate_queue.log

# Webhook is NOT stored in this tracked file: read it from the untracked
# droplet run script, or fall back to the SLACK_WEBHOOK env var.
eval "$(grep -m1 '^SLACK_WEBHOOK=' run_house_2022_2024.sh 2>/dev/null || true)"
slack(){ [ -n "${SLACK_WEBHOOK:-}" ] && \
  curl -s -X POST "$SLACK_WEBHOOK" -H 'Content-type: application/json' \
  -d "{\"text\":\"$1\"}" >/dev/null 2>&1; }

echo "$(date '+%F %T') === senate queue started (${#} rosters) ===" >> "$QLOG"
slack "FYI: [$HOST] Senate scrape queue started ($# rosters)."

for roster in "$@"; do
    if [ ! -f "$roster" ]; then
        echo "$(date '+%F %T') SKIP missing $roster" >> "$QLOG"
        slack "ACTION NEEDED: [$HOST] Senate queue skipped missing roster $roster."
        continue
    fi
    stem=$(basename "$roster" .csv)
    n=$(( $(wc -l < "$roster") - 1 ))
    echo "$(date '+%F %T') START $stem ($n candidates)" >> "$QLOG"
    set +e
    python -m src.scrape_wayback --roster "$roster" --threads 1 --log-level INFO \
        >> "logs/${stem}.log" 2>&1
    rc=$?
    set -e 2>/dev/null || true
    echo "$(date '+%F %T') DONE  $stem (exit $rc)" >> "$QLOG"
    if [ "$rc" -eq 0 ]; then slack "FYI: [$HOST] Senate queue finished $stem (exit 0)."
    else slack "ACTION NEEDED: [$HOST] Senate queue $stem FAILED (exit $rc). Check logs/${stem}.log."; fi
done

echo "$(date '+%F %T') === senate queue complete ===" >> "$QLOG"
slack "FYI / no action: [$HOST] Senate scrape queue COMPLETE. Ready for tar/verify checkpoints."
