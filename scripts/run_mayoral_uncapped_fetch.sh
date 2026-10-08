#!/bin/bash
# Fetch the pages the capped mayoral site scrapes left out (Hanno, October 8, 2026), one batch at a time.
# Run on mac2 from ~/candidate_website_extension_stage3 after scripts/build_mayoral_uncapped_queue.py wrote
# runs/uncapped/fetch_queue.csv in each batch. Same fetch settings as the stage 3 batches.
cd ~/candidate_website_extension_stage3
A=data/municipal_archive_acquisition
. scripts/slack_status.sh
export SLACK_REPO=hhilbig/candidate_website_extension HOST_LABEL=mac2
BATCHES="mayoral_stage3_20261002 mayoral_stage3b_20261003 mayoral_stage3L_20261004 mayoral_stage3r_20261004 covered_151_20260826 full_discovery_740_20260826 openai_residual_expansion_357_20260826"
slack_status FYI "Candidate websites, mayoral extension" "Wayback page download, all pages beyond the old 150-page cap" \
  "Started. 7 batches with about 9,300 pages for 44 campaign sites." "About 4 to 6 hours, rough until the first batch ends." "log uncapped_fetch.log"
for b in $BATCHES; do
  B=$A/$b
  echo "$(date -Is) start $b"
  BATCH=$B BATCH_LABEL="uncapped pages, $b" RUN_NAME=uncapped LOG_NAME=uncapped.log QUIET_START=1 \
    nohup python3 scripts/watch_mayoral_stage3_scrape.py > $B/uncapped_watch.log 2>&1 < /dev/null &
  PYTHONPATH=. .venv/bin/python scripts/scrape_mayoral_wayback_batch.py fetch --targets $B/frozen/targets.csv \
    --manifest $B/frozen/manifest.json --run-dir $B/runs/uncapped --workers 2 --rate-limit-seconds 1.0 \
    --max-raw-bytes 20000000000 --log-level INFO > $B/uncapped.log 2>&1
  rc=$?
  echo "$(date -Is) end $b rc=$rc"
done
echo "$(date -Is) all batches done"
