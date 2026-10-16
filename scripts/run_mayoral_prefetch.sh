#!/bin/bash
# Fetch the pre-election copies queued by scripts/build_mayoral_prefetch_queue.py, one batch at a time,
# with the stage 3 fetch settings (2 workers, 1 second rate limit). Run on mac2 from ~/candidate_website_extension_stage3:
#   nohup caffeinate -i bash scripts/run_mayoral_prefetch.sh > release_mayoral/prefetch.log 2>&1 &
cd ~/candidate_website_extension_stage3
JS=~/.local/bin/jobstatus; ID=mayoral-prefetch-pre-election
BATCHES=$(ls -d data/municipal_archive_acquisition/*/runs/prefetch | cut -d/ -f3)
TOTAL=$(cat data/municipal_archive_acquisition/*/runs/prefetch/fetch_queue.csv | grep -vc '^capture_id,')
$JS set $ID --project "Candidate websites, mayoral extension" --task "Wayback download of pre-election page copies" \
  --total "$TOTAL" --unit pages --log release_mayoral/prefetch.log --note "Started."
fail=0
for NAME in $BATCHES; do
  B=data/municipal_archive_acquisition/$NAME
  echo "$(date +%Y-%m-%dT%H:%M:%S) start $NAME"
  PYTHONPATH=. .venv/bin/python scripts/scrape_mayoral_wayback_batch.py fetch --targets $B/frozen/targets.csv \
    --manifest $B/frozen/manifest.json --run-dir $B/runs/prefetch --workers 2 --rate-limit-seconds 1.0 \
    --max-raw-bytes 20000000000 --log-level INFO > $B/prefetch.log 2>&1
  rc=$?
  [ $rc -ne 0 ] && fail=1
  DONE=$(cat data/municipal_archive_acquisition/*/runs/prefetch/fetch_attempts.csv 2>/dev/null | grep -vc '^capture_id,')
  echo "$(date +%Y-%m-%dT%H:%M:%S) end $NAME rc=$rc"
  $JS set $ID --done "$DONE" --note "Batch $NAME ended, rc $rc."
done
if [ $fail -eq 0 ]; then $JS set $ID --state finished --note "All batches done."
else $JS set $ID --state failed --note "At least one batch ended with an error; see release_mayoral/prefetch.log."; fi
echo "$(date +%Y-%m-%dT%H:%M:%S) all done fail=$fail"
