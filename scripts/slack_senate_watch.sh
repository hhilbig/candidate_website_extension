#!/usr/bin/env bash
# Slack-on-completion / stall watcher for the Senate scrape queue.
#
# Posts ONE message when the queue finishes, or an ACTION-NEEDED message if
# the scraper process dies with no completion line, or if the queue log goes
# silent for >STALL seconds (the 6h Wayback auto-pause can stall it quietly).
# Reads the webhook from the untracked droplet run script (never embedded).
#
# Usage: nohup scripts/slack_senate_watch.sh >logs/slack_senate_watch.out 2>&1 &
set -u
cd ~/candidate_website_extension
HOST=$(hostname)
QLOG=logs/senate_queue.log
STALL=$(( 7 * 3600 ))   # 7h: longer than the 6h auto-pause, so a real stall

eval "$(grep -m1 '^SLACK_WEBHOOK=' run_house_2022_2024.sh 2>/dev/null || true)"
slack(){ [ -n "${SLACK_WEBHOOK:-}" ] && \
  curl -s -X POST "$SLACK_WEBHOOK" -H 'Content-type: application/json' \
  -d "{\"text\":\"$1\"}" >/dev/null 2>&1; }

stalled=0
for i in $(seq 1 1440); do          # up to ~24h
  if grep -q "senate queue complete" "$QLOG" 2>/dev/null; then
    exit 0                          # run_senate_queue already sent the success ping
  fi
  # process gone with no completion line = died/killed
  if ! pgrep -f "scrape_wayback --roster .*senate" >/dev/null 2>&1; then
    sleep 30
    if ! grep -q "senate queue complete" "$QLOG" 2>/dev/null \
       && ! pgrep -f "scrape_wayback --roster .*senate" >/dev/null 2>&1; then
      slack "ACTION NEEDED: [$HOST] Senate queue process GONE with no completion line — likely crashed/killed. Check logs/senate_queue.log."
      exit 1
    fi
  fi
  # silent-stall detection
  if [ -f "$QLOG" ]; then
    idle=$(( $(date +%s) - $(stat -c %Y "$QLOG") ))
    active=$(ls -t logs/roster_senate_*.log logs/senate_*.log 2>/dev/null | head -1)
    [ -n "$active" ] && idle=$(( $(date +%s) - $(stat -c %Y "$active") ))
    if [ "$idle" -ge "$STALL" ] && [ "$stalled" -eq 0 ]; then
      stalled=1
      slack "ACTION NEEDED: [$HOST] Senate queue STALLED ${idle}s with no log activity. Check Wayback / the run."
    fi
    [ "$idle" -lt "$STALL" ] && stalled=0
  fi
  sleep 60
done
slack "ACTION NEEDED: [$HOST] Senate queue watcher timed out after ~24h. Check the run."
exit 2
