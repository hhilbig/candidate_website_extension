#!/usr/bin/env python3
"""Export exact-target metadata and verified saved bytes without archive requests."""
import argparse
import collections
import csv
from datetime import datetime, timezone
import gzip
import hashlib
import json
from pathlib import Path
import re
import shlex
import sqlite3
import subprocess
import sys
from urllib.parse import urlsplit, urlunsplit

FIELDS = ['candidate_url_id', 'candidate_year_id', 'exact_normalized_url',
          'capture_original_url', 'capture_timestamp', 'raw_saved', 'text_saved',
          'raw_path', 'text_path', 'raw_sha256', 'text_sha256', 'source_ref',
          'capture_id', 'evidence_kind', 'hash_status']


def sha(path):
    digest = hashlib.sha256()
    with Path(path).open('rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            digest.update(block)
    return digest.hexdigest()


def exact(url):
    """Normalize host case only; preserve scheme, path case, www and query."""
    value = (url or '').strip()
    match = re.match(r'https?://(?:web\.archive\.org/web|wayback\.archive-it\.org/[^/]+)/\d{14}[a-z_]*(?:/)(https?://.*)', value)
    if match:
        value = match[1]
    try:
        parsed = urlsplit(value)
        if parsed.scheme not in {'http', 'https'} or not parsed.hostname or parsed.username:
            return ''
        return urlunsplit((parsed.scheme, parsed.netloc.lower(), parsed.path or '/', parsed.query, ''))
    except ValueError:
        return ''


def rows(path):
    with Path(path).open(newline='') as stream:
        return list(csv.DictReader(stream))


def scan(payload):
    """Runs on the corpus host. Every operation is read-only."""
    targets = payload['targets']
    lookup = collections.defaultdict(list)
    for target in targets:
        normalized = exact(target['exact_normalized_url'])
        if normalized:
            lookup[(target['candidate_year_id'], normalized)].append(target)
    out, sources, audits = [], {}, []
    checks = {}

    def check(path, expected):
        key = (str(path), expected)
        if key not in checks:
            if not expected or not path.is_file():
                checks[key] = ('missing_or_no_hash', False)
            else:
                try:
                    opener = gzip.open if path.suffix == '.gz' else open
                    with opener(path, 'rb') as stream:
                        content = stream.read()
                    checks[key] = ('verified' if hashlib.sha256(content).hexdigest() == expected else 'hash_mismatch', bool(content))
                except (OSError, EOFError):
                    checks[key] = ('read_error', False)
        return checks[key]

    def add(candidate, url, timestamp, ref, capture_id='', kind='inventory', raw=None, text=None, rh='', th=''):
        matched = lookup.get((candidate, exact(url)), [])
        if not matched or not re.fullmatch(r'\d{14}', timestamp or ''):
            return
        rs, rn = check(raw, rh) if raw else ('not_recorded', False)
        ts, tn = check(text, th) if text else ('not_recorded', False)
        for target in matched:
            out.append(dict(candidate_url_id=target['candidate_url_id'], candidate_year_id=candidate,
                exact_normalized_url=target['exact_normalized_url'], capture_original_url=exact(url),
                capture_timestamp=timestamp, raw_saved=rs == 'verified' and rn,
                text_saved=ts == 'verified' and tn, raw_path=str(raw) if raw else '',
                text_path=str(text) if text else '', raw_sha256=rh, text_sha256=th,
                source_ref=ref, capture_id=capture_id, evidence_kind=kind,
                hash_status='raw:' + rs + ';text:' + ts))

    def load(path):
        if not path.is_file():
            audits.append({'path': str(path), 'status': 'missing'})
            return []
        before = sha(path)
        result = rows(path)
        after = sha(path)
        if before != after:
            raise ValueError('Source changed during read: ' + str(path))
        sources[str(path)] = before
        audits.append({'path': str(path), 'status': 'read', 'rows': len(result)})
        return result

    volume = Path(payload.get('volume', '/mnt/mayoral_wayback_full_snapshots'))
    acquisition = Path(payload.get('acquisition', '/root/candidate_website_extension/data/municipal_archive_acquisition'))
    roots = [volume / 'runs/production_all_inventory']
    roots += [acquisition / name / 'runs/production' for name in (
        'covered_151_20260826', 'openai_residual_100_20260826',
        'openai_residual_expansion_357_20260826', 'full_discovery_740_20260826')]
    for root in roots:
        path = root / 'capture_inventory.csv'
        for row in load(path):
            add(row['candidate_year_id'], row['original_url'], row['timestamp'], str(path), row['capture_id'])
        path = root / 'fetch_attempts.csv'
        for row in load(path):
            if row['fetch_status'] not in {'fetched', 'fetched_timestamp_mismatch', 'no_content'}:
                continue
            add(row['candidate_year_id'], row.get('effective_replay_url', ''), row.get('effective_timestamp', ''),
                str(path), row['capture_id'], 'saved_response',
                root / row['raw_html_path'] if row.get('raw_html_path') else None,
                root / row['text_path'] if row.get('text_path') else None,
                row.get('raw_html_sha256', ''), row.get('text_sha256', ''))

    path = volume / 'ownership_validation_20260910/results/validation.csv'
    for row in load(path):
        digest = row.get('capture_content_sha256', '')
        if digest:
            add(row['candidate_year_id'], row.get('capture_effective_url', ''), row.get('capture_effective_timestamp', ''),
                str(path), digest, 'validation_saved_response', path.parent / 'evidence' / (digest + '.gz'), None, digest, '')

    root = volume / 'exact_capture_20260914'
    queue = {r['proposed_capture_key']: r for r in load(root / 'inputs/exact_capture_queue.csv')}
    for key, row in queue.items():
        add(row['candidate_year_id'], row['original_url'], row['timestamp'], str(root / 'inputs/exact_capture_queue.csv'), key)
    path = root / 'run/ledger.sqlite3'
    if path.is_file():
        db = sqlite3.connect('file:' + str(path) + '?mode=ro', uri=True)
        db.row_factory = sqlite3.Row
        records = [dict(r) for r in db.execute('SELECT key,status,result FROM current ORDER BY key')]
        db.close()
        sources[str(path) + '#current_logical_sha256'] = hashlib.sha256(json.dumps(records, sort_keys=True).encode()).hexdigest()
        for record in records:
            row = queue.get(record['key'])
            if not row or record['status'] not in {'fetched', 'no_content', 'capture_mismatch'}:
                continue
            result = json.loads(record['result'] or '{}')
            hops = result.get('redirects', [])
            last = hops[-1] if hops else {}
            add(row['candidate_year_id'], result.get('effective_original_url', ''), result.get('effective_timestamp', ''),
                str(path) + '#current:' + record['key'], record['key'], 'exact_batch_saved_response',
                root / 'run' / last['raw_path'] if last.get('raw_path') else None,
                root / 'run' / result['text_path'] if result.get('text_path') else None,
                last.get('raw_sha256', ''), result.get('text_sha256', ''))
    path = volume / 'inventory_22_20260914/run/ledger.sqlite3'
    if path.is_file():
        before = sha(path)
        db = sqlite3.connect('file:' + str(path) + '?mode=ro', uri=True)
        db.row_factory = sqlite3.Row
        queries = [dict(r) for r in db.execute('SELECT * FROM queries ORDER BY query_key')]
        captures = [dict(r) for r in db.execute('SELECT * FROM capture_rows WHERE accepted=1 ORDER BY row_id')]
        db.close()
        if sha(path) != before:
            raise ValueError('Inventory ledger changed during read')
        sources[str(path)] = before
        by_query = {r['query_key']: r for r in queries}
        start_count = len(out)
        for row in captures:
            query = by_query[row['query_key']]
            if query['status'] not in {'COMPLETE', 'EMPTY'}:
                continue
            add(query['candidate_year_id'], row['original'], row['timestamp'],
                str(path) + '#capture_rows:' + str(row['row_id']), str(row['row_id']), 'targeted_inventory')
        audits.append(dict(path=str(path), status='read', rows=len(captures),
                           query_status_counts=dict(collections.Counter(r['status'] for r in queries)),
                           matched_evidence_rows=len(out)-start_count,
                           excluded_reason='Returned URL variants outside the exact target match remain in the source ledger.'))
    unique = {tuple(str(r[f]) for f in FIELDS): r for r in out}
    return {'rows': sorted(unique.values(), key=lambda r: (r['candidate_url_id'], r['capture_timestamp'], r['source_ref'])),
            'source_hashes': sources, 'source_audit': audits,
            'file_check_counts': dict(collections.Counter(status for status, _ in checks.values())),
            'limits': ['Exact scheme/host/path/query and same candidate-year only; variants excluded.',
                       'File hashes refer to decompressed saved bytes; acquisition does not prove campaign identity.',
                       'Validation raw responses without saved text cannot count as downloaded text.',
                       'Only explicit listed ledgers inspected; absence is not archive absence.',
                       'Snapshot dates are actual inventory or effective replay timestamps. No election window applied here.']}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--targets', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--host', default='root@droplet')
    parser.add_argument('--local', action='store_true')
    args = parser.parse_args()
    target_hash = sha(args.targets)
    targets = rows(args.targets)
    payload = {'targets': [{k: r[k] for k in ('candidate_url_id', 'candidate_year_id', 'exact_normalized_url')} for r in targets]}
    if len({r['candidate_url_id'] for r in targets}) != len(targets):
        raise ValueError('Duplicate target IDs')
    if args.local:
        result = scan(payload)
    else:
        code = Path(__file__).read_text().rsplit("if __name__ == '__main__':", 1)[0]
        code += '\nprint(json.dumps(scan(json.load(sys.stdin))))\n'
        command = ['ssh', '-o', 'BatchMode=yes', '-o', 'ConnectTimeout=15', args.host, 'python3 -c ' + shlex.quote(code)]
        proc = subprocess.run(command, input=json.dumps(payload), text=True, capture_output=True)
        if proc.returncode:
            raise RuntimeError(proc.stderr)
        result = json.loads(proc.stdout)
    if sha(args.targets) != target_hash:
        raise ValueError('Targets changed during the evidence export')
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open('w', newline='') as stream:
        writer = csv.DictWriter(stream, fieldnames=FIELDS)
        writer.writeheader()
        writer.writerows(result.pop('rows'))
    saved = rows(args.output)
    result.update(target_path=str(args.targets), target_sha256=target_hash, target_rows=len(targets),
                  output_sha256=sha(args.output), evidence_rows=len(saved),
                  distinct_target_ids=len({r['candidate_url_id'] for r in saved}), host=args.host,
                  script_sha256=sha(__file__), exported_at_utc=datetime.now(timezone.utc).isoformat())
    args.output.with_suffix('.manifest.json').write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps({k: result[k] for k in ('evidence_rows', 'distinct_target_ids', 'file_check_counts')}, indent=2))


if __name__ == '__main__':
    main()
