#!/bin/bash
# Daily 2026 campaign-website collection.
#
# Installed 2026-09-20 after the collection was found idle for five days: full
# mode was enabled on 2026-09-16 and nothing ran after 2026-09-15. The nominal
# owner was a daily Codex task, `collect-2026-campaign-websites`, which left no
# trace on this host. This script is now the owner. Do not add a second one.
#
# One worker only. If a collector is already running, this exits and waits for
# the next day rather than running two against the same queue.
set -u
# The URL waterfall reads this variable. Without it openfec falls back to
# DEMO_KEY and every lookup returns no_registered_key, which is what happened
# to 3,302 of 3,689 candidates before 2026-09-23. Never echo the key.
if [ -r /root/.openfec_api_key ]; then
    OPENFEC_API_KEY="$(cat /root/.openfec_api_key)"
    export OPENFEC_API_KEY
else
    echo "$(date -Is) FATAL no openfec key, refusing to run a blind lookup" >&2
    exit 1
fi

R=/mnt/mayoral_wayback_full_snapshots/candidate_2026/runtime
DATA=../data
cd "$R" || exit 1
mkdir -p logs

if pgrep -f "collect_2026\.py (run|refresh)" >/dev/null; then
    echo "$(date -Is) skip, a collector is already running" >> logs/daily.log
    exit 0
fi

DAY=$(date -u +%Y%m%d)
PT_DAY=$(TZ=America/Los_Angeles date +%F)
PT_HOUR=$(TZ=America/Los_Angeles date +%H)

# General election, Hanno 2026-10-24: on 2026-11-02 and 2026-11-04 (the local days before and
# after the November 3 election) cron also starts this script at 00:05 Pacific. That early run (until 08:20,
# so its export and status step end before 09:00) stops before the regular run; the 09:00 run gets 12 hours and ends near Eastern midnight.
# The collector decides what is due at run start, so a run must not continue into the next local
# day (election day is excluded from the comparison). The Monday refresh takes about 4 hours, so
# the refresh of the week of November 2 runs on Sunday, November 1.
MAX_RUN=21600
if [ "$PT_DAY" = "2026-11-02" ] || [ "$PT_DAY" = "2026-11-04" ]; then
    if [ "$PT_HOUR" -lt 9 ]; then MAX_RUN=29700; else MAX_RUN=43200; fi
fi
DO_REFRESH=0
[ "$(date -u +%u)" = "1" ] && DO_REFRESH=1
[ "$PT_DAY" = "2026-11-02" ] && DO_REFRESH=0
[ "$PT_DAY" = "2026-11-01" ] && DO_REFRESH=1
[ "$PT_HOUR" -lt 9 ] && DO_REFRESH=0

# The roster and the URL list refresh weekly, on Mondays (see the November exception above).
if [ "$DO_REFRESH" = "1" ]; then
    # --lookup-limit 0 means candidates[:0], which is NO lookups at all, not
    # unlimited. Every Monday refresh before 2026-09-24 looked up nothing.
    # 4000 covers the whole roster of 3,689 with headroom for growth.
    ./.venv/bin/python scripts/collect_2026.py refresh --root "$DATA" \
        --lookup-limit 4000 >> "logs/refresh_$DAY.log" 2>&1
    rc=$?
    echo "$(date -Is) refresh finished rc=$rc" >> logs/daily.log
fi

# The collector decides what is due. It does not copy every site every day.
./.venv/bin/python scripts/collect_2026.py run --root "$DATA" --phase all \
    --max-run-seconds "$MAX_RUN" >> "logs/run_$DAY.log" 2>&1
# Save the exit code first. Inside "$(date -Is) ... $?" the shell runs date
# before expanding $?, so until 2026-09-30 every line read rc=0, including
# seven runs that stopped on a ValueError.
rc=$?
echo "$(date -Is) run finished rc=$rc max_run=$MAX_RUN" >> logs/daily.log

./.venv/bin/python scripts/collect_2026.py status --root "$DATA" \
    > "logs/status_$DAY.json" 2>&1
