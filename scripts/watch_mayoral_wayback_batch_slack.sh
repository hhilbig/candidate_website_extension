#!/bin/bash
# Send one terminal Slack alert for a detached mayoral Wayback batch, in the
# job-alerts format (level, Project, Task, Status, ETA, Where).
#
# usage: watch_mayoral_wayback_batch_slack.sh JOB_PID RUN_DIR LOG_PATH [POLL_SECONDS]
#        watch_mayoral_wayback_batch_slack.sh --preview RUN_DIR   (dry run, no post)

set -u

cd "$(dirname "$0")/.." || exit 1
. scripts/slack_status.sh
export SLACK_REPO="${SLACK_REPO:-hhilbig/candidate_website_extension}"
export HOST_LABEL="${HOST_LABEL:-droplet}"

if [ "${1:-}" = "--preview" ]; then
    run_dir="${2:?usage: --preview RUN_DIR}"
    log_path="preview.log"
    export SLACK_DRY_RUN=1
else
    job_pid="$1"
    run_dir="$2"
    log_path="$3"
    poll_seconds="${4:-60}"
    while kill -0 "$job_pid" 2>/dev/null; do
        sleep "$poll_seconds"
    done
fi

# One line each: level, status sentence, eta sentence (bash 3.2 has no readarray).
{ read -r level; read -r status_line; read -r eta_line; } < <(python3 - "$run_dir" <<'PY'
import json
import sys
from pathlib import Path

run_dir = Path(sys.argv[1])
inv_path = run_dir / "inventory_audit.json"
fetch_path = run_dir / "fetch_audit.json"
if not inv_path.exists() or not fetch_path.exists():
    print("ACTION NEEDED")
    print("Stopped without complete audit files. The batch did not finish cleanly.")
    print("Unknown. Check the log and rerun the batch.")
    raise SystemExit
inv = json.loads(inv_path.read_text())
fetch = json.loads(fetch_path.read_text())
retryable = int(fetch.get("retryable_rows", -1))
complete = (
    inv.get("stage") == "inventory_complete"
    and inv.get("exact_variant_attempt_key_agreement") is True
    and fetch.get("stage") == "fetch_complete"
    and fetch.get("exact_queue_key_agreement") is True
    and retryable == 0
)
targets = inv.get("targets", "unknown")
saved = fetch.get("fetched_rows", "unknown")
def fmt(x):
    return f"{x:,}" if isinstance(x, int) else str(x)
if complete:
    print("FYI")
    print(f"Finished. {fmt(targets)} targets, {fmt(saved)} pages saved.")
    print("Done. Next is the extraction step.")
else:
    print("ACTION NEEDED")
    problem = f"{fmt(retryable)} pages still retryable" if retryable > 0 else "an audit check did not agree"
    print(f"Finished with a problem: {problem}. {fmt(targets)} targets, {fmt(saved)} pages saved.")
    print("Unknown. Rerun the batch for the retryable pages.")
PY
)

batch="$(basename "$(dirname "$(dirname "$run_dir")")" 2>/dev/null)"
[ -z "$batch" ] || [ "$batch" = "." ] && batch="$(basename "$run_dir")"
slack_status "$level" "Mayoral candidates" "Wayback batch, $batch" \
    "$status_line" "$eta_line" "run $(basename "$run_dir"), log $(basename "$log_path")"
