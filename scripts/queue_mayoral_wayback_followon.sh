#!/bin/bash
# Wait for one existing acquisition wrapper, check disk, then run a frozen batch.

set -u

wait_pid="$1"
batch_root="$2"
log_path="$3"
min_free_kb="${4:-2500000}"
repo_root="/root/candidate_website_extension"
run_dir="${batch_root}/runs/production"

while kill -0 "$wait_pid" 2>/dev/null; do
    sleep 60
done

cd "$repo_root" || exit 70

free_kb="$(df -Pk "$repo_root" | awk 'NR==2 {print $4}')"
if [ -z "$free_kb" ] || [ "$free_kb" -lt "$min_free_kb" ]; then
    echo "ACTION NEEDED: insufficient free disk for queued mayoral Wayback batch; free_kb=${free_kb:-unknown}, required_kb=$min_free_kb" >> "$log_path"
    exit 75
fi

mkdir -p "$run_dir" "$(dirname "$log_path")"
echo "Starting queued mayoral Wayback batch; free_kb=$free_kb" >> "$log_path"

exec env PYTHONPATH=. .venv/bin/python scripts/scrape_mayoral_wayback_batch.py run \
    --targets "${batch_root}/frozen/targets.csv" \
    --manifest "${batch_root}/frozen/manifest.json" \
    --run-dir "$run_dir" \
    --rate-limit-seconds 1 \
    --attempts 5 \
    --connect-timeout 30 \
    --read-timeout 120 \
    --max-raw-bytes 1500000000 \
    --circuit-breaker-failures 5 \
    --circuit-breaker-sleep 900 \
    --log-level INFO >> "$log_path" 2>&1
