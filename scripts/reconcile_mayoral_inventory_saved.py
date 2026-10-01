"""Match the finished inventory to hash-verified saved responses; no HTTP requests."""
import argparse
import collections
import csv
import gzip
import hashlib
import json
from pathlib import Path
import re
import sqlite3
import subprocess
from urllib.parse import urlsplit, urlunsplit


def sha(path):
    h = hashlib.sha256()
    with path.open('rb') as f:
        for block in iter(lambda: f.read(1048576), b''):
            h.update(block)
    return h.hexdigest()


def exact(url):
    try:
        p = urlsplit(url or '')
        _ = p.port
    except ValueError:
        return ''
    if p.scheme not in {'http', 'https'} or not p.hostname or p.username or p.password:
        return ''
    return urlunsplit((p.scheme, p.netloc.lower(), p.path or '/', p.query, ''))


def variant(url):
    p = urlsplit(exact(url))
    port = p.port
    if (p.scheme, port) in {('http', 80), ('https', 443)}:
        port = None
    return p.hostname.removeprefix('www.') if p.hostname else '', port, p.path or '/', p.query


def replay(url):
    p = urlsplit(url or '')
    if p.hostname not in {'web.archive.org', 'wayback.archive-it.org'}:
        return None
    match = re.search(r'/(\d{14})[a-z_]*/(https?://.*)', p.path + ('?' + p.query if p.query else ''))
    if not match:
        return None
    return p.hostname, match[1], exact(match[2])


def match_records(records, evidence):
    indexed = collections.defaultdict(list)
    for item in evidence:
        indexed[(item['timestamp'], variant(item['original_url']))].append(item)
    result = []
    seen_rows = {}
    for row in records:
        identity = tuple(row[k] for k in ('query_key', 'urlkey', 'timestamp', 'original', 'mimetype', 'statuscode', 'digest', 'length'))
        matches = indexed.get((row['timestamp'], variant(row['original'])), [])
        matches = [m for m in matches if not m.get('candidate_year_id') or m['candidate_year_id'] == row['candidate_year_id']]
        direct = [m for m in matches if m['provider'] == 'web.archive.org' and exact(m['original_url']) == exact(row['original']) and m['raw_verified']]
        alternatives = [m for m in matches if m not in direct]
        result.append(dict(row, duplicate_of_row_id=seen_rows.get(identity, ''),
            saved_exact_response=bool(direct), saved_exact_text=any(m['text_verified'] for m in direct),
            disposition='verified_saved_exact_response' if direct else 'no_verified_exact_response_in_inspected_stores',
            exact_evidence=direct, alternative_or_unverified_evidence=alternatives))
        seen_rows.setdefault(identity, row['row_id'])
    return result


def scan(payload):
    root = Path(payload['inventory_root'])
    ledger = root / 'run/ledger.sqlite3'
    queue = root / 'inputs/targeted_inventory_queue.csv'
    if sha(ledger) != payload['ledger_sha256'] or sha(queue) != payload['queue_sha256']:
        raise ValueError('Verified inventory inputs changed')
    db = sqlite3.connect(f'file:{ledger}?mode=ro', uri=True)
    db.row_factory = sqlite3.Row
    if any(r[0] not in {'COMPLETE', 'EMPTY'} for r in db.execute('SELECT status FROM queries')):
        raise ValueError('Inventory is not complete')
    records = [dict(r) for r in db.execute('''SELECT c.*,q.candidate_url_id,q.candidate_year_id,q.requested_url
        FROM capture_rows c JOIN queries q USING(query_key) WHERE c.accepted=1 ORDER BY c.row_id''')]
    db.close()
    if len(records) != payload['accepted_cdx_rows']:
        raise ValueError('Inventory record count differs')
    wanted = {(r['timestamp'], variant(r['original'])) for r in records}
    evidence, inspections, errors, checks = [], [], [], {}

    def check(path, digest):
        k = str(path), digest
        if k not in checks:
            try:
                data = path.read_bytes()
                if path.suffix == '.gz':
                    data = gzip.decompress(data)
                checks[k] = dict(verified=bool(digest) and hashlib.sha256(data).hexdigest() == digest,
                                 decoded_bytes=len(data), stored_bytes=path.stat().st_size)
            except (OSError, EOFError) as exc:
                checks[k] = dict(verified=False, error=str(exc))
        return checks[k]

    def add(url, ref, raw_path, raw_hash, candidate='', text_path=None, text_hash='', http_status=''):
        parsed = replay(url)
        if not parsed:
            return
        provider, timestamp, original = parsed
        if not original or (timestamp, variant(original)) not in wanted:
            return
        raw = check(raw_path, raw_hash) if raw_path else {'verified': False}
        text = check(text_path, text_hash) if text_path else {'verified': False}
        evidence.append(dict(provider=provider, timestamp=timestamp, original_url=original,
            candidate_year_id=candidate, candidate_binding='source_record' if candidate else 'URL_and_timestamp_only',
            source_ref=ref, raw_path=str(raw_path or ''), raw_sha256=raw_hash,
            raw_verified=raw['verified'], raw_check=raw, text_path=str(text_path or ''),
            text_sha256=text_hash, text_verified=text['verified'] and text.get('decoded_bytes', 0) > 0,
            http_status=str(http_status)))

    volume = Path('/mnt/mayoral_wayback_full_snapshots')
    acquisition = Path('/root/candidate_website_extension/data/municipal_archive_acquisition')
    files = set()
    for store in (volume, acquisition):
        files.update(store.rglob('fetch_attempts.csv'))
        files.update(store.rglob('fetch_attempts.journal.csv'))
    for path in sorted(files):
        before = sha(path)
        count = 0
        with path.open(newline='') as f:
            for row in csv.DictReader(f):
                count += 1
                if row.get('raw_html_path'):
                    add(row.get('effective_replay_url', ''), f'{path}:{count+1}',
                        path.parent / row['raw_html_path'], row.get('raw_html_sha256', ''),
                        row.get('candidate_year_id', ''),
                        path.parent / row['text_path'] if row.get('text_path') else None,
                        row.get('text_sha256', ''), row.get('http_status', ''))
        if sha(path) != before:
            raise ValueError(f'Ledger changed during scan: {path}')
        inspections.append(dict(path=str(path), sha256=before, rows=count, kind='fetch_ledger'))
    for path in sorted(volume.rglob('requests.jsonl')):
        before = sha(path)
        count = 0
        with path.open() as f:
            for number, line in enumerate(f, 1):
                try:
                    row = json.loads(line)
                except ValueError:
                    errors.append(dict(path=str(path), line=number, error='invalid_json'))
                    continue
                count += 1
                if row.get('evidence_file'):
                    add(row.get('effective_url', ''), f'{path}:{number}',
                        path.parent / row['evidence_file'], row.get('sha256', ''),
                        http_status=row.get('status', ''))
        if sha(path) != before:
            raise ValueError(f'Journal changed during scan: {path}')
        inspections.append(dict(path=str(path), sha256=before, rows=count, kind='response_journal'))
    # Independent provenance format: validation bindings can survive journal gaps.
    path = volume / 'ownership_validation_20260910/results/validation.csv'
    if path.exists():
        before = sha(path)
        count = 0
        with path.open(newline='') as f:
            for row in csv.DictReader(f):
                count += 1
                digest = row.get('capture_content_sha256', '')
                if digest:
                    add(row.get('capture_effective_url', ''), f'{path}:{count+1}',
                        path.parent / 'evidence' / (digest + '.gz'), digest,
                        row.get('candidate_year_id', ''))
        if sha(path) != before:
            raise ValueError('Validation ledger changed during scan')
        inspections.append(dict(path=str(path), sha256=before, rows=count, kind='validation_bindings'))
    # Explicit newly completed SQLite acquisition format, separate from CDX metadata.
    path = volume / 'exact_capture_20260914/run/ledger.sqlite3'
    if path.exists():
        before = sha(path)
        connection = sqlite3.connect(f'file:{path}?mode=ro', uri=True)
        data = connection.execute('SELECT key,status,result FROM current').fetchall()
        connection.close()
        for key, status, encoded in data:
            if status not in {'fetched', 'capture_mismatch', 'no_content'}:
                continue
            row = json.loads(encoded)
            last = row['redirects'][-1]
            add(row.get('effective_replay_url', ''), f'{path}#current:{key}',
                path.parent / last['raw_path'], last['raw_sha256'],
                text_path=path.parent / row['text_path'] if row.get('text_path') else None,
                text_hash=row.get('text_sha256', ''), http_status=last.get('status_code', ''))
        if sha(path) != before:
            raise ValueError('Exact batch changed during scan')
        inspections.append(dict(path=str(path), sha256=before, rows=len(data), kind='exact_batch_sqlite'))
    if sha(ledger) != payload['ledger_sha256']:
        raise ValueError('Inventory changed during scan')
    return dict(records=match_records(records, evidence), inspected_sources=inspections,
                scan_errors=errors, evidence_records=len(evidence), files_checked=len(checks),
                verified_files=sum(c['verified'] for c in checks.values()),
                limitation='Scans named fetch CSVs, response journals, ownership-validation bindings, and exact-batch SQLite. Uncatalogued orphan files and other ledger formats remain outside this match. Variant URLs and Archive-It responses are alternatives, not exact Wayback acquisitions.')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--verification', type=Path, required=True)
    parser.add_argument('--inventory-root', required=True)
    parser.add_argument('--output-dir', type=Path, required=True)
    args = parser.parse_args()
    if args.output_dir.exists():
        raise ValueError('Refusing to overwrite previous reconciliation')
    verified = json.loads(args.verification.read_text())
    payload = {k: verified[k] for k in ('ledger_sha256', 'queue_sha256', 'accepted_cdx_rows')}
    payload['inventory_root'] = args.inventory_root
    source = Path(__file__).read_text().split('\ndef main():')[0]
    proc = subprocess.run(['ssh', '-o', 'BatchMode=yes', '-o', 'ConnectTimeout=15', 'root@droplet', 'python3', '-'],
        input=source + '\nprint(json.dumps(scan(' + repr(payload) + ')))\n', text=True,
        capture_output=True, timeout=300, check=True)
    result = json.loads(proc.stdout)
    records = result.pop('records')
    assert len({r['row_id'] for r in records}) == len(records) == verified['accepted_cdx_rows']
    args.output_dir.mkdir(parents=True)
    output = args.output_dir / 'inventory_saved_matches.json'
    output.write_text(json.dumps(records, indent=2) + '\n')
    summary = dict(result, inventory_rows=len(records),
        duplicate_inventory_rows=sum(r['duplicate_of_row_id'] != '' for r in records),
        saved_exact_rows=sum(r['saved_exact_response'] for r in records),
        saved_exact_text_rows=sum(r['saved_exact_text'] for r in records),
        unmatched_rows=sum(not r['saved_exact_response'] for r in records),
        statuses=dict(collections.Counter(r['disposition'] for r in records)),
        unmatched_by_index_status=dict(collections.Counter(r['statuscode'] for r in records if not r['saved_exact_response'])),
        matched_rows_sha256=sha(output), verification_input_sha256=sha(args.verification),
        script_sha256=sha(Path(__file__)), archive_requests=0, raw_files_copied_to_mac=0)
    (args.output_dir / 'summary.json').write_text(json.dumps(summary, indent=2) + '\n')
    print(json.dumps({k:v for k,v in summary.items() if k != 'inspected_sources'}, indent=2))


if __name__ == '__main__':
    main()
