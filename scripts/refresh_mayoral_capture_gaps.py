"""Reconcile the frozen completeness audit with the completed exact batch.

Read archived bytes on the droplet only. Retain frozen proposal inputs and write
compact current gap tables separately, without copying the raw corpus to Mac.
"""
import collections
import csv
import json
from pathlib import Path
import subprocess

from scripts import audit_mayoral_capture_completeness as audit


BASE = Path('quality_reports/mayoral_capture_completeness_2026-09-14')
REMOTE_BATCH = '/mnt/mayoral_wayback_full_snapshots/exact_capture_20260914'


def key(row):
    return row['candidate_year_id'], row['timestamp'], audit.exact(row['original_url'])


def main():
    output = BASE / 'after_exact_acquisition'
    if output.exists():
        raise ValueError('Output already exists; do not overwrite a frozen refresh')
    queue_path = BASE / 'proposal/exact_capture_queue.csv'
    manifest = json.loads((BASE / 'proposal/manifest.json').read_text())
    assert audit.sha(queue_path) == manifest['output_sha256']
    queue = list(audit.rows(queue_path))
    keys = {key(row) for row in queue}
    assert len(keys) == len(queue) == manifest['selected_capture_keys']
    remote_path = BASE / 'remote_evidence.json'
    baseline = json.loads(remote_path.read_text())
    selected = [row for row in baseline['captures'] if key(row) in keys]
    assert len(selected) == len(keys)
    payload = dict(captures=selected, hashes={}, raw_files_checked=0, raw_files_verified=0)
    source_path = Path(audit.__file__)
    source = source_path.read_text().split('\ndef main():')[0]
    command = source + '\nprint(json.dumps(merge_exact_batch(' + repr(payload) + ', Path(' + repr(REMOTE_BATCH) + '))))\n'
    result = subprocess.run(['ssh', '-o', 'BatchMode=yes', '-o', 'ConnectTimeout=15',
                             'root@droplet', 'python3', '-'], input=command,
                            text=True, capture_output=True, timeout=300, check=True)
    supplemental = json.loads(result.stdout)
    updated = {key(row): row for row in supplemental['captures']}
    assert set(updated) == keys and len(updated) == len(supplemental['captures'])
    baseline['captures'] = [updated.get(key(row), row) for row in baseline['captures']]
    prior_summary = json.loads((BASE / 'summary.json').read_text())
    assert audit.sha(audit.INPUT) == prior_summary['input_sha256']
    requests = [row for row in audit.rows(audit.INPUT) if row['ai_accepted_website'].lower() == 'true']
    rows, retry, inventories = audit.summarize(requests, baseline)
    old_retry = list(audit.rows(BASE / 'targeted_capture_retry.csv'))
    old_keys, new_keys = {key(row) for row in old_retry}, {key(row) for row in retry}
    assert old_keys - new_keys == keys and not new_keys - old_keys
    assert len(old_keys) == len(old_retry) and len(new_keys) == len(retry)
    output.mkdir()
    audit.write_csv(output / 'targeted_capture_retry.csv', retry)
    audit.write_csv(output / 'url_capture_audit.csv', rows)
    audit.write_csv(output / 'targeted_inventory_queue.csv', inventories)
    inputs = [remote_path, queue_path, BASE / 'summary.json',
              BASE / 'targeted_capture_retry.csv', audit.INPUT, source_path, Path(__file__)]
    summary = dict(input_hashes={str(p): audit.sha(p) for p in inputs},
                   remote_verified_input_hashes=supplemental['hashes'],
                   previous_unsaved_keys=len(old_keys), remaining_unsaved_keys=len(new_keys),
                   completed_exact_keys_removed=len(keys), unexpected_key_changes=0,
                   remaining_by_relation=dict(collections.Counter(row['url_relation'] for row in retry)),
                   inventory_queue_urls=len(inventories), accepted_urls=len(requests),
                   new_raw_files_verified=supplemental['raw_files_verified'],
                   archive_requests=0, raw_files_copied_to_mac=0,
                   output_hashes={p.name: audit.sha(p) for p in output.iterdir() if p.is_file()},
                   limitation='Updates only the approved exact batch against the frozen audit. Remaining inventories and other-path acquisitions are not authorized by this refresh.')
    (output / 'summary.json').write_text(json.dumps(summary, indent=2) + '\n')
    print(json.dumps(summary, indent=2))


if __name__ == '__main__':
    main()
