"""Prepare an offline acquisition proposal from audited capture gaps."""
import csv
import hashlib
import json
from collections import Counter
from pathlib import Path
from urllib.parse import urlsplit

SOURCE = Path('quality_reports/mayoral_capture_completeness_2026-09-14')
OUTPUT = SOURCE / 'proposal'


def prepare(rows):
    keys = [(r['candidate_year_id'], r['timestamp'], r['original_url']) for r in rows]
    if len(keys) != len(set(keys)):
        raise ValueError('Duplicate capture key')
    selected, deferred = [], []
    for row in rows:
        if row['url_relation'] != 'exact':
            deferred.append(row)
            continue
        if row['reason'] != 'not_queued':
            raise ValueError('Exact proposal contains a non-acquisition case')
        url = urlsplit(row['original_url'])
        if (url.scheme not in {'http', 'https'} or not url.hostname
                or url.username or url.password or len(row['timestamp']) != 14
                or not row['timestamp'].isdigit()):
            raise ValueError('Invalid capture target')
        identity = {k: row[k] for k in ['candidate_year_id', 'timestamp', 'original_url']}
        identity['provider'] = 'web.archive.org'
        key = hashlib.sha256(json.dumps(identity, sort_keys=True).encode()).hexdigest()
        selected.append({**row, 'proposed_capture_key': key,
                         'requested_replay_url': f"https://web.archive.org/web/{row['timestamp']}id_/{row['original_url']}",
                         'proposal_status': 'awaiting_launch_approval'})
    return sorted(selected, key=lambda r: r['proposed_capture_key']), deferred


def main():
    queue = SOURCE / 'targeted_capture_retry.csv'
    summary_path = SOURCE / 'summary.json'
    with queue.open() as handle:
        original = list(csv.DictReader(handle))
    summary = json.loads(summary_path.read_text())
    selected, deferred = prepare(original)
    assert len(original) == 2233 and len(selected) == 259
    OUTPUT.mkdir(exist_ok=True)
    with (OUTPUT / 'exact_capture_queue.csv').open('x', newline='') as handle:
        writer = csv.DictWriter(handle, fieldnames=list(selected[0]))
        writer.writeheader()
        writer.writerows(selected)
    estimate = summary['byte_estimates_by_relation']['exact']
    manifest = dict(status='proposal_only_not_launched', selected_capture_keys=len(selected),
        upstream_capture_keys=len(original), deferred_capture_keys=len(deferred),
        deferred_by_relation=dict(Counter(r['url_relation'] for r in deferred)),
        candidate_years=len({r['candidate_year_id'] for r in selected}),
        observed_mean_estimate_raw_bytes=estimate['raw_bytes'],
        observed_mean_estimate_compressed_bytes=estimate['compressed_bytes'],
        estimate_excludes='Extracted text, metadata, retry bodies, and new inventory results.',
        proposed_limits=dict(workers=1, attempts_per_capture=3, max_response_decoded_bytes=2097152,
            max_tranche_decoded_bytes=134217728, min_volume_free_bytes=8000000000),
        scope='Never-queued exact original URL and timestamp keys from saved inventories. Preserve all deferred rows upstream.',
        time_windows='Retain the recorded inventory timestamps without imposing a new selection window.',
        target_equivalence='Only literal requested original URL targets qualify; retain changed effective targets as unresolved.',
        timestamp_policy='Retain substitutions as evidence but do not count them as acquisition of the requested timestamp.',
        prerequisites=['Approve this bounded acquisition scope before launch.',
            'Verify current saved keys again immediately before launch.',
            'Enforce persistent request and decoded-byte limits before any network request.',
            'Use an archive-only redirect policy and preserve requested/effective provenance.',
            'Keep raw responses and extracted text on the attached volume.'],
        input_hashes={str(p): hashlib.sha256(p.read_bytes()).hexdigest() for p in [queue, summary_path]},
        script_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        output_sha256=hashlib.sha256((OUTPUT / 'exact_capture_queue.csv').read_bytes()).hexdigest(),
        archive_requests=0)
    with (OUTPUT / 'manifest.json').open('x') as handle:
        json.dump(manifest, handle, indent=2)
        handle.write('\n')
    print(json.dumps(manifest, indent=2))


if __name__ == '__main__':
    main()
