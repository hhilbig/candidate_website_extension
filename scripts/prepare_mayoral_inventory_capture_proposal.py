"""Prepare an offline proposal; retain the original index and exact URL variants."""
import argparse
import collections
import csv
import hashlib
import json
from pathlib import Path

from scripts.reconcile_mayoral_inventory_saved import exact


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def prepare(records):
    if len({r['row_id'] for r in records}) != len(records):
        raise ValueError('Duplicate inventory row ID')
    selected = {}
    dispositions = []
    for row in records:
        key = (row['candidate_year_id'], row['timestamp'], row['original'])
        if not exact(row['original']):
            raise ValueError('Invalid original URL')
        if row['saved_exact_response']:
            reason = 'verified_saved_response'
        elif row['statuscode'] == '200' and row['mimetype'] == 'text/html':
            reason = 'propose_html_capture'
        elif row['mimetype'] == 'warc/revisit':
            reason = 'propose_revisit_capture'
        else:
            reason = 'retain_redirect_or_error_metadata_for_separate_review'
        dispositions.append(dict(row_id=row['row_id'], disposition=reason))
        if not reason.startswith('propose_'):
            continue
        if key not in selected:
            identity = dict(candidate_year_id=key[0], timestamp=key[1], original_url=key[2],
                            provider='web.archive.org')
            selected[key] = dict(identity,
                proposed_capture_key=hashlib.sha256(json.dumps(identity, sort_keys=True).encode()).hexdigest(),
                requested_replay_url=f"https://web.archive.org/web/{key[1]}id_/{key[2]}",
                candidate_url_id=row['candidate_url_id'], inventory_requested_url=row['requested_url'],
                url_relation='exact' if exact(key[2]) == exact(row['requested_url']) else 'index_returned_url_variant',
                reason='no_verified_exact_response_in_inspected_stores',
                selection_basis=reason, inventory_row_ids=[],
                proposal_status='awaiting_approval_not_executable')
        selected[key]['inventory_row_ids'].append(row['row_id'])
    result = sorted(selected.values(), key=lambda r:r['proposed_capture_key'])
    for row in result:
        row['inventory_row_ids_json'] = json.dumps(sorted(row.pop('inventory_row_ids')))
    return result, dispositions


def run(source, output):
    if output.exists():
        raise ValueError('Refusing to replace an existing proposal')
    summary_path = source / 'summary.json'
    records_path = source / 'inventory_saved_matches.json'
    summary = json.loads(summary_path.read_text())
    if sha(records_path) != summary['matched_rows_sha256']:
        raise ValueError('Reconciliation hash differs')
    records = json.loads(records_path.read_text())
    if len(records) != summary['inventory_rows']:
        raise ValueError('Reconciliation count differs')
    queue, dispositions = prepare(records)
    if not queue:
        raise ValueError('No proposed captures')
    output.mkdir(parents=True)
    queue_path = output / 'proposed_captures.csv'
    with queue_path.open('x', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=list(queue[0]))
        writer.writeheader()
        writer.writerows(queue)
    decisions_path = output / 'inventory_dispositions.json'
    decisions_path.write_text(json.dumps(dispositions, indent=2)+'\n')
    manifest = dict(status='proposal_only_requires_batch_approval',
        source_rows=len(records), selected_capture_keys=len(queue),
        row_dispositions=dict(collections.Counter(r['disposition'] for r in dispositions)),
        selected_by_basis=dict(collections.Counter(r['selection_basis'] for r in queue)),
        selected_url_relations=dict(collections.Counter(r['url_relation'] for r in queue)),
        candidate_years=len({r['candidate_year_id'] for r in queue}),
        proposed_limits=dict(workers=1, max_attempts_per_capture=3,
            max_response_decoded_bytes=2097152, max_batch_decoded_bytes=134217728,
            min_volume_free_bytes=8000000000),
        selection='All unmatched HTML-success and revisit keys in the completed 22-URL inventory; no timestamp or digest sampling.',
        exclusions='Saved responses do not need another download. Redirect/error records remain upstream for separate review; they are not archive-absence findings.',
        scope='Preserve each indexed original URL, including scheme/www/default-port variants. Require exact effective URL and timestamp for success.',
        byte_estimate='Unknown before acquisition. The 128 MiB decoded cap is a stop limit, not an estimate or guarantee that all keys fit.',
        preconditions=['Explicit batch approval.', 'Fresh saved-key and available-space checks before launch.',
            'Use a separately frozen runner contract that permits indexed URL variants; do not relabel variants as exact accepted URLs.',
            'Preserve substitutions, failures, and limit-stopped keys; do not count them as completed requested captures.',
            'Save raw responses and text on the attached volume; copy only compact audits to the Mac.'],
        inputs={str(p):sha(p) for p in (summary_path, records_path)},
        outputs={str(p):sha(p) for p in (queue_path, decisions_path)},
        script_sha256=sha(Path(__file__)), archive_requests=0)
    (output/'manifest.json').write_text(json.dumps(manifest, indent=2)+'\n')
    return manifest


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(run(args.source, args.output), indent=2))
