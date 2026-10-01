"""Record host-local process and audit state without sending notifications."""
import argparse
import json
import os
from pathlib import Path
import time


def process_identity(pid):
    try:
        fields = Path(f'/proc/{pid}/stat').read_text().rsplit(')', 1)[1].split()
        return None if fields[0] == 'Z' else fields[19]
    except (OSError, IndexError):
        return None


def watch(pid, run_dir):
    identity = process_identity(pid)
    started = time.time()
    while True:
        running = identity is not None and process_identity(pid) == identity
        audit_path = run_dir / 'audit.json'
        try:
            audit = json.loads(audit_path.read_text())
            last_activity = audit_path.stat().st_mtime
        except (OSError, ValueError):
            audit, last_activity = {}, started
        state = dict(pid=pid, process_start_ticks=identity, checked_at=time.time(),
                     running=running, stalled=running and time.time()-last_activity > 1800,
                     attempts=audit.get('attempts'), statuses=audit.get('statuses'),
                     stop_reason=audit.get('stop_reason'),
                     acquisition_complete=audit.get('acquisition_complete', False),
                     inventory_complete=audit.get('inventory_complete', False),
                     external_notifications=False)
        temporary = run_dir / 'watcher.json.tmp'
        with temporary.open('w') as stream:
            json.dump(state, stream, indent=2)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, run_dir / 'watcher.json')
        if not running:
            return state
        time.sleep(30)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--pid', type=int, required=True)
    parser.add_argument('--run-dir', type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(watch(args.pid, args.run_dir)))
