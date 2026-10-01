#!/usr/bin/env python3
"""Supervise the detached mayoral source pass and report its final state."""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0,str(ROOT))
from scripts.expand_mayoral_saved_sources import atomic_json, now


def completion_message(audit, exit_code, host, run):
    combined = audit.get("combined_roster",{})
    unfinished = audit.get("never_attempted_sources",0)
    needs_action = exit_code != 0 or unfinished > 0
    count = combined.get('new_active_candidate_years_with_source_attributed_leads', 'unknown')
    status = (f"Failed with exit code {exit_code}. Check the source log." if exit_code else
              f"Finished this tranche. Source expansion has {count} new candidate-year discovery proposals since the original baseline. "
              + ("Review the results before another tranche." if unfinished else "The source queue is complete."))
    return alert_message("ACTION NEEDED" if needs_action else "FYI, no action needed", status,
                         "Done. Next is review of the new URL evidence.", host, run)


def alert_message(level, status, eta, host, run):
    return (f"{level}\nProject: Mayoral URL discovery\nTask: Source-page discovery\n"
            f"Status: {status}\nETA: {eta}\n"
            f"Where: {host}, repo github.com/hhilbig/candidate_website_extension, log {run}/source_pass.log")


def build_command(args, run):
    command = [sys.executable, str(ROOT / "scripts/expand_mayoral_saved_sources.py"), "fetch",
               "--repo-root", str(ROOT), "--run-dir", str(run), "--workers", str(args.workers)]
    for name in ("tranche_id", "additional_bytes", "max_sources", "min_free_bytes"):
        value = getattr(args, name, None)
        if value is not None:
            command += ["--" + name.replace("_", "-"), str(value)]
    return command


def activity_age(run, started):
    # Progress writes alone do not establish source progress.
    paths = [run / name for name in ("source_pass.log", "sources.sqlite", "sources.sqlite-wal")]
    return time.time() - max([started] + [p.stat().st_mtime for p in paths if p.exists()])


def notify(helper, message):
    # The existing helper reads the secret locally; it never enters Python or logs.
    subprocess.run(["/bin/bash","-c",'source "$1"; slack_notify "$2"',"slack",str(helper),message],
                   check=True,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,timeout=30)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-dir",type=Path,required=True)
    parser.add_argument("--slack-helper",type=Path,required=True)
    parser.add_argument("--workers",type=int,default=8)
    parser.add_argument("--tranche-id")
    parser.add_argument("--additional-bytes", type=int)
    parser.add_argument("--max-sources", type=int)
    parser.add_argument("--min-free-bytes", type=int, default=8_000_000_000)
    args = parser.parse_args()
    run = args.run_dir.resolve()
    import fcntl
    with (run/".supervisor.lock").open("a") as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        command = build_command(args, run)
        state = {"project":"candidate_website_extension","job":"mayoral_saved_source_expansion_20260908",
                 "host":socket.gethostname(),"supervisor_pid":os.getpid(),"started_at":now(),"status":"starting","command":command}
        atomic_json(run/"job_state.json",state)
        with (run/"source_pass.log").open("a") as log:
            child = subprocess.Popen(command,cwd=ROOT,stdout=log,stderr=subprocess.STDOUT)
            state.update(child_pid=child.pid,status="running")
            atomic_json(run/"job_state.json",state)
            stall_notified = False
            started = time.time()
            last_notification = started
            host = "droplet" if sys.platform == "linux" else "mac2"
            try:
                notify(args.slack_helper, alert_message("FYI, no action needed", "Started the approved source tranche.",
                       "Unknown until enough source documents finish.", host, run))
                state["start_notification"] = "helper_invoked"
            except Exception as error:
                state["notification_error"] = type(error).__name__
            while child.poll() is None:
                time.sleep(30)
                stale_seconds = activity_age(run, started)
                if stale_seconds > 1800 and not stall_notified:
                    message = alert_message("ACTION NEEDED", "Stalled. The log and source database show no recent activity. Check the source log.",
                                            "Unknown until it resumes.", host, run)
                    try:
                        notify(args.slack_helper,message)
                        stall_notified = True
                    except Exception as error:
                        state["notification_error"] = type(error).__name__
                elif stale_seconds <= 1800:
                    stall_notified = False
                if not stall_notified and time.time() - last_notification >= 21600:
                    try:
                        notify(args.slack_helper, alert_message("FYI, no action needed",
                               f"Running for {(time.time()-started)/3600:.1f} hours. The source database has recent activity.",
                               "Unknown until the remaining source workload is measured.", host, run))
                        last_notification = time.time()
                    except Exception as error:
                        state["notification_error"] = type(error).__name__
                state.update(checked_at=now(),progress_stale_seconds=int(stale_seconds))
                atomic_json(run/"job_state.json",state)
        code = child.returncode
        audit = json.loads((run/"audit.json").read_text()) if (run/"audit.json").is_file() else {}
        state.update(exit_code=code,status="finished" if code==0 else "failed",finished_at=now())
        atomic_json(run/"job_state.json",state)
        message = completion_message(audit,code,host,run)
        atomic_json(run/"completion_notification.json",{"created_at":now(),"message":message,"status":"pending"})
        try:
            notify(args.slack_helper,message)
            atomic_json(run/"completion_notification.json",{"created_at":now(),"message":message,"status":"helper_invoked"})
        except Exception as error:
            atomic_json(run/"completion_notification.json",{"created_at":now(),"message":message,"status":"helper_error","error":type(error).__name__})
        return code


if __name__=="__main__":
    raise SystemExit(main())
