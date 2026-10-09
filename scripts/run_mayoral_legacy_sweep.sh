#!/bin/bash
# Domain-pattern sweep for finalists whose only lead is an old roster URL without an archived copy
# (approved by Hanno October 9, 2026). Runs on the droplet after the daily 2026 collector, before and
# after the election, then screens the hits. Reports to jobstatus.
W=/mnt/mayoral_wayback_full_snapshots/coverage_sweep_20261002
D=data/municipal_discovery/top100_coverage/url_discovery/legacy_search_20261009
PY=/root/candidate_website_extension/.venv/bin/python
JS=~/.local/bin/jobstatus
cd $W
$JS set mayoral-legacy-sweep --project "Candidate websites, mayoral extension" --task "Domain search for finalists with an outdated roster URL" --total 4 --unit steps --log $W/$D/sweep.log --note "Waiting for the daily 2026 collector to finish."
while pgrep -f 'collect_2026.py (run|refresh)' >/dev/null; do sleep 120; done
n=0
for w in before after; do
  echo "$(date +%Y-%m-%dT%H:%M:%S) wayback $w"
  $PY scripts/sweep_mayoral_domain_patterns.py wayback --sweep-dir $D/sweep_$w --window $w --gap 3; rc=$?
  n=$((n+1)); $JS set mayoral-legacy-sweep --done $n --note "Archive search $w the election done, rc $rc."
  $PY scripts/sweep_mayoral_domain_patterns.py screen --sweep-dir $D/sweep_$w --window $w --gap 3; rc=$?
  n=$((n+1)); $JS set mayoral-legacy-sweep --done $n --note "Page screen $w the election done, rc $rc."
done
$JS set mayoral-legacy-sweep --done 4 --state finished --note "Both windows searched and screened."
echo "$(date +%Y-%m-%dT%H:%M:%S) all done"
