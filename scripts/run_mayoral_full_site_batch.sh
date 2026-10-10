#!/bin/bash
# Scrape one mayoral batch completely: the capped stage 3 run, then every left-out content page
# (scripts/build_mayoral_uncapped_queue.py). Run on mac2 from ~/candidate_website_extension_stage3:
#   bash scripts/run_mayoral_full_site_batch.sh <batch folder name> "<label>"
# Reports to jobstatus; the stage 3 Slack watcher covers the capped run.
cd ~/candidate_website_extension_stage3
NAME=$1; LABEL=$2; B=data/municipal_archive_acquisition/$NAME
JS=~/.local/bin/jobstatus; ID=mayoral-scrape-$NAME
ARGS="--targets $B/frozen/targets.csv --manifest $B/frozen/manifest.json --workers 2 --rate-limit-seconds 1.0 --max-raw-bytes 20000000000 --log-level INFO"
$JS set $ID --project "Candidate websites, mayoral extension" --task "Wayback page download, $LABEL" --total 2 --unit steps --log $B/runs.log --note "Capped run started."
BATCH=$B BATCH_LABEL="$LABEL" nohup caffeinate -i python3 scripts/watch_mayoral_stage3_scrape.py > $B/watch.log 2>&1 < /dev/null &
PYTHONPATH=. .venv/bin/python scripts/scrape_mayoral_wayback_batch.py run $ARGS --run-dir $B/runs/production > $B/runs.log 2>&1
rc=$?
$JS set $ID --done 1 --note "Capped run ended, rc $rc. Building the uncapped queue."
python3 scripts/build_mayoral_uncapped_queue.py --batch-dir $B > $B/uncapped_queue.log 2>&1
PYTHONPATH=. .venv/bin/python scripts/scrape_mayoral_wayback_batch.py fetch $ARGS --run-dir $B/runs/uncapped > $B/uncapped.log 2>&1
rc2=$?
if [ $rc -eq 0 ] && [ $rc2 -eq 0 ]; then $JS set $ID --done 2 --state finished --note "Capped and uncapped runs done."
else $JS set $ID --state failed --note "Capped run rc $rc, uncapped run rc $rc2."; fi
echo "$(date +%Y-%m-%dT%H:%M:%S) done rc=$rc rc2=$rc2"
