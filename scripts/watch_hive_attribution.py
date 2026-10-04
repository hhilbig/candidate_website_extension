"""Slack watcher for the Hive legacy-lead attribution jobs (job-alerts format).

Runs on the primary Mac: Hive accepts SSH from here, and the Slack webhook lives on
mac2, so messages go through `ssh mac2` and the job-alerts helper there. Posts when a
copy starts, every 6 hours while all copies wait, once when a copy finishes (and
cancels the other copy), and once if every copy fails. Then it exits.
  nohup caffeinate -i python3 scripts/watch_hive_attribution.py JOBID [JOBID ...] &
Set WATCH_DRY_RUN=1 to print instead of posting.
"""
from __future__ import annotations

import os
import shlex
import subprocess
import sys
import time

HIVE = "hhilbig@hive.hpc.ucdavis.edu"
WORK = "/nfs/hive/scratch/hhilbig/mayoral_site_attribution"
MAC2_HELPER = "~/candidate_website_extension_stage3/scripts/slack_status.sh"
PROJECT = "Candidate websites, mayoral extension"
TASK = "Hive check of saved pages for older mayoral URL leads"
POLL, PENDING_EVERY = 120, 6 * 3600


def hive(command: str) -> str:
    result = subprocess.run(["ssh", "-o", "ConnectTimeout=30", HIVE, f"bash -lc {shlex.quote(command)}"],
                            capture_output=True, text=True, timeout=180)
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip()[:200])
    return result.stdout


def post(level: str, status: str, eta: str, where: str) -> None:
    args = [level, PROJECT, TASK, status, eta, where]
    remote = (f"SLACK_REPO=hhilbig/candidate_website_extension HOST_LABEL=hive "
              f"{'SLACK_DRY_RUN=1 ' if os.environ.get('WATCH_DRY_RUN') else ''}"
              f"bash -c '. {MAC2_HELPER}; slack_status \"$@\"' _ " + " ".join(shlex.quote(a) for a in args))
    output = subprocess.run(["ssh", "mac2", remote], capture_output=True, text=True).stdout
    if output:
        print(output, flush=True)


def states(jobs: list[str]) -> dict[str, tuple[str, str]]:
    out = {}
    for line in hive(f"sacct -n -X -P -j {','.join(jobs)} -o JobID,State,Partition").splitlines():
        job, state, partition = line.split("|")
        out[job] = (state.split()[0], partition)
    return out


def counts(output: str) -> str:
    return hive(f"python3 -c \"import json,collections; c=collections.Counter(json.loads(l).get('decision','none') "
                f"for l in open('{output}') if not json.loads(l).get('gold')); print(c['yes'], c['no'], c['unclear'])\"").strip()


def main(jobs: list[str]) -> None:
    outputs = {jobs[0]: f"{WORK}/decisions.jsonl"} | {j: f"{WORK}/decisions_low.jsonl" for j in jobs[1:]}
    started, last_pending, began = False, time.time(), time.time()
    while True:
        time.sleep(POLL)
        try:
            current = states(jobs)
        except Exception as exc:  # transient SSH failure: try again next poll
            print("poll failed:", exc, flush=True)
            continue
        running = [j for j, (s, _) in current.items() if s == "RUNNING"]
        if running and not started:
            post("FYI", f"Started on the {current[running[0]][1]} partition. 243 items, about 15 minutes of GPU time.",
                 "About 20 minutes from now.", f"partition {current[running[0]][1]}, job {running[0]}")
            started = True
        done = [j for j, (s, _) in current.items() if s == "COMPLETED"]
        if done:
            job = done[0]
            others = [j for j in jobs if j != job and current.get(j, ("", ""))[0] in {"PENDING", "RUNNING"}]
            if others:
                hive("scancel " + " ".join(others))
            yes, no, unclear = counts(outputs[job]).split()
            post("FYI", f"Finished. Of 157 older URL leads, {yes} are confirmed as campaign sites, {no} rejected, "
                        f"{unclear} unclear.", "Done. Next is the calibration check and the coverage rebuild.",
                 f"partition {current[job][1]}, job {job}")
            return
        if all(s not in {"PENDING", "RUNNING", "REQUEUED", "CONFIGURING", "COMPLETING"} for s, _ in current.values()):
            post("ACTION NEEDED", "Failed. No copy of the job completed.", "Unknown until it is resubmitted.",
                 f"check the logs mayoral-site-attribution_{jobs[0]}.log in home")
            return
        if not running and time.time() - last_pending >= PENDING_EVERY:
            post("FYI", f"Still waiting for a GPU after {(time.time() - began) / 3600:.1f} hours. The shared GPU budget is full.",
                 "Unknown until a GPU frees up.", f"jobs {' and '.join(jobs)}")
            last_pending = time.time()


if __name__ == "__main__":
    main(sys.argv[1:])
