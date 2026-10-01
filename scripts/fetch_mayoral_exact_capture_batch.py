"""Serial, bounded acquisition of a frozen exact Wayback capture queue.

Stored and hashed bytes are requests.iter_content client-decoded response bytes,
not WARC bytes. Every HTTP hop reserves its full decoded ceiling durably first.
An interrupted hop retains that charge; completed hops refund unused capacity.
"""
from __future__ import annotations

import argparse
import csv
import fcntl
import gzip
import hashlib
from html.parser import HTMLParser
import json
import os
from pathlib import Path
import re
import shutil
import sqlite3
import time
from urllib.parse import urljoin, urlsplit
import uuid

import requests

LIMITS = dict(workers=1, attempts_per_capture=3,
              max_response_decoded_bytes=2097152,
              max_tranche_decoded_bytes=134217728,
              min_volume_free_bytes=8000000000)
MAX_REDIRECTS = 5
RETRYABLE = {408, 425, 429, 500, 502, 503, 504}
LEGACY_CONTRACT_VERSION = 'mayoral_exact_capture_batch_legacy_v1'
INDEXED_CONTRACT_VERSION = 'mayoral_indexed_capture_batch_v1'
INDEXED_RELATIONS = {'exact', 'index_returned_url_variant'}
APPROVED_INDEXED_QUEUES = {
    ('4eeaafc8681c0664673b1eab239053aacff8c67eb8b39d788289665991d12418',
     'db9faf097048af9582fc7b322d744ab5dc5b6f19e1f333b44d17832f1bf60620'),
    # The 439-capture proposal from the 19-URL inventory. Hanno approved it on
    # 2026-09-30, with the same limits as the 214-capture batch.
    ('9a8cfa723d8aeaa665382b8ee4ac44eb7e4e2c54e0aef7fd56c6610761f60aaa',
     '57e6f4b25eed4166b8669daa016ddfab06a50b9623654ee1c175b09b93d6aa70'),
}


def capture_key(row):
    identity = {k: row[k] for k in ('candidate_year_id', 'timestamp', 'original_url')}
    identity['provider'] = 'web.archive.org'
    return hashlib.sha256(json.dumps(identity, sort_keys=True).encode()).hexdigest()


def replay_identity(url):
    parsed = urlsplit(url)
    if (parsed.scheme != 'https' or parsed.hostname != 'web.archive.org'
            or parsed.username or parsed.password or parsed.port not in (None, 443)):
        raise ValueError('non-archive redirect blocked')
    if parsed.fragment:
        raise ValueError('invalid archive replay URL')
    match = re.fullmatch(r'https://web\.archive\.org/web/(\d{14})(?:id_)?/(https?://.+)', url)
    if not match:
        raise ValueError('invalid archive replay URL')
    return match[1], match[2]


class TextParser(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.hidden = 0
        self.parts = []

    def handle_starttag(self, tag, attrs):
        if tag in {'script', 'style', 'noscript'}:
            self.hidden += 1

    def handle_endtag(self, tag):
        if tag in {'script', 'style', 'noscript'} and self.hidden:
            self.hidden -= 1

    def handle_data(self, data):
        if not self.hidden:
            self.parts.append(data)


def atomic_write(path, data):
    temporary = path.with_name(path.name + '.tmp')
    with temporary.open('wb') as handle:
        handle.write(data)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)
    directory = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)


def comparable_original_url(url):
    parsed = urlsplit(url)
    if (parsed.scheme not in {'http', 'https'} or not parsed.hostname
            or parsed.username or parsed.password):
        raise ValueError('invalid original URL')
    try:
        port = parsed.port
    except ValueError as exc:
        raise ValueError('invalid original URL') from exc
    default_port = 80 if parsed.scheme == 'http' else 443
    if port not in (None, default_port):
        raise ValueError('indexed URL variant has a non-default port')
    host = parsed.hostname.lower()
    if host.startswith('www.'):
        host = host[4:]
    return host, parsed.path or '/', parsed.query


def exact_normalized_original_url(url):
    parsed = urlsplit(url)
    if (parsed.scheme not in {'http', 'https'} or not parsed.hostname
            or parsed.username or parsed.password):
        raise ValueError('invalid original URL')
    try:
        port = parsed.port
    except ValueError as exc:
        raise ValueError('invalid original URL') from exc
    netloc = parsed.hostname.lower() + (f':{port}' if port is not None else '')
    return parsed.scheme, netloc, parsed.path or '/', parsed.query


def load_queue(queue, manifest):
    payload = queue.read_bytes()
    digest = hashlib.sha256(payload).hexdigest()
    manifest_payload = manifest.read_bytes()
    manifest_digest = hashlib.sha256(manifest_payload).hexdigest()
    frozen = json.loads(manifest_payload)
    version = frozen.get('contract_version', LEGACY_CONTRACT_VERSION)
    if version == LEGACY_CONTRACT_VERSION:
        expected_digest = frozen.get('output_sha256')
        expected_limits = frozen.get('proposed_limits')
    elif version == INDEXED_CONTRACT_VERSION:
        approval = frozen.get('approval', {})
        expected_digest = frozen.get('approved_queue_sha256')
        expected_limits = frozen.get('approved_limits')
        approved_pair = (expected_digest, frozen.get('proposal_manifest_sha256'))
        approved_batches = approval.get('approved_batches')
        if (approval.get('status') != 'approved' or approval.get('approved_by') != 'user'
                or not isinstance(approved_batches, dict)
                or set(approved_batches) != INDEXED_RELATIONS
                or any(not isinstance(count, int) or count < 0 for count in approved_batches.values())
                or frozen.get('approved_url_relations') != sorted(INDEXED_RELATIONS)
                or not frozen.get('preserve_original_proposal_bytes')
                or approved_pair not in APPROVED_INDEXED_QUEUES):
            raise ValueError('indexed capture contract is not explicitly approved')
    else:
        raise ValueError('unsupported queue contract version')
    if digest != expected_digest or expected_limits != LIMITS:
        raise ValueError('frozen queue hash or approved limits differ')
    rows = list(csv.DictReader(payload.decode('utf-8').splitlines()))
    if len(rows) != frozen['selected_capture_keys']:
        raise ValueError('frozen queue row count differs')
    keys = set()
    relation_counts = {relation: 0 for relation in INDEXED_RELATIONS}
    for row in rows:
        key = capture_key(row)
        if key != row['proposed_capture_key'] or key in keys:
            raise ValueError('duplicate or invalid capture key')
        keys.add(key)
        expected = f"https://web.archive.org/web/{row['timestamp']}id_/{row['original_url']}"
        original = urlsplit(row['original_url'])
        if version == LEGACY_CONTRACT_VERSION:
            row_scope = row['url_relation'] == 'exact' and row['reason'] == 'not_queued'
        else:
            if row['url_relation'] not in INDEXED_RELATIONS:
                raise ValueError('queue contains an invalid URL relation')
            row_scope = (row['url_relation'] in INDEXED_RELATIONS
                         and row['reason'] == 'no_verified_exact_response_in_inspected_stores'
                         and row['proposal_status'] == 'awaiting_approval_not_executable')
            accepted_url = row.get('accepted_url') or row.get('inventory_requested_url')
            equivalent = (accepted_url
                          and comparable_original_url(row['original_url']) == comparable_original_url(accepted_url))
            if not equivalent:
                raise ValueError('indexed URL variant differs beyond scheme/www/default-port form')
            exactly_equivalent = (exact_normalized_original_url(row['original_url'])
                                  == exact_normalized_original_url(accepted_url))
            if row['url_relation'] == 'exact' and not exactly_equivalent:
                raise ValueError('exact URL relation does not match accepted URL')
            if row['url_relation'] == 'index_returned_url_variant' and exactly_equivalent:
                raise ValueError('indexed URL variant is not a variant')
            relation_counts[row['url_relation']] += 1
        if (row['requested_replay_url'] != expected or not row_scope
                or original.scheme not in {'http', 'https'}
                or not original.hostname or original.username or original.password
                or replay_identity(expected) != (row['timestamp'], row['original_url'])):
            raise ValueError('queue contains an out-of-scope target')
    if version == INDEXED_CONTRACT_VERSION and relation_counts != approval['approved_batches']:
        raise ValueError('approved batch counts differ from queue')
    contract = dict(contract_version=version, queue_sha256=digest, limits=LIMITS,
                    max_redirects=MAX_REDIRECTS)
    if version == INDEXED_CONTRACT_VERSION:
        contract.update(approval_manifest_sha256=manifest_digest,
                        proposal_manifest_sha256=frozen['proposal_manifest_sha256'])
    return rows, digest, contract


def open_ledger(path, rows, digest, queue_contract=None):
    db = sqlite3.connect(path)
    db.row_factory = sqlite3.Row
    db.execute('PRAGMA synchronous=FULL')
    db.executescript('''
        CREATE TABLE IF NOT EXISTS contract (id INTEGER PRIMARY KEY, value TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS current (key TEXT PRIMARY KEY, status TEXT NOT NULL,
          attempts INTEGER NOT NULL DEFAULT 0, next_due REAL NOT NULL DEFAULT 0, result TEXT);
        CREATE TABLE IF NOT EXISTS attempts (id TEXT PRIMARY KEY, key TEXT NOT NULL,
          started REAL NOT NULL, finished REAL, status TEXT NOT NULL, result TEXT);
        CREATE TABLE IF NOT EXISTS hops (attempt_id TEXT NOT NULL, hop INTEGER NOT NULL,
          url TEXT NOT NULL, charged INTEGER NOT NULL, decoded INTEGER, result TEXT,
          PRIMARY KEY(attempt_id, hop));
        CREATE TABLE IF NOT EXISTS control (id INTEGER PRIMARY KEY, failures INTEGER NOT NULL,
          circuit_until REAL NOT NULL, next_request REAL NOT NULL);
    ''')
    # Preserve the byte-for-byte legacy resume contract when no explicit new
    # queue contract is supplied.
    contract = json.dumps(queue_contract or dict(queue_sha256=digest, limits=LIMITS,
                                                 max_redirects=MAX_REDIRECTS), sort_keys=True)
    old = db.execute('SELECT value FROM contract').fetchone()
    if old and old[0] != contract:
        raise ValueError('resume contract differs')
    with db:
        db.execute('INSERT OR IGNORE INTO contract VALUES (1, ?)', (contract,))
        db.execute('INSERT OR IGNORE INTO control VALUES (1, 0, 0, 0)')
        db.executemany("INSERT OR IGNORE INTO current(key,status) VALUES (?, 'pending')",
                       [(r['proposed_capture_key'],) for r in rows])
        expected = {r['proposed_capture_key'] for r in rows}
        observed = {r[0] for r in db.execute('SELECT key FROM current')}
        if observed != expected:
            raise ValueError('unexpected or missing current-state keys')
        interrupted = list(db.execute("SELECT id,key FROM attempts WHERE status='started'"))
        for attempt in interrupted:
            db.execute("UPDATE attempts SET status='interrupted', finished=? WHERE id=?", (time.time(), attempt['id']))
            db.execute("UPDATE current SET status='retryable', next_due=? WHERE key=?", (time.time()+30, attempt['key']))
    return db


class BudgetStop(Exception):
    pass


def fetch(db, session, row, attempt_id, output, sleep=time.sleep):
    url = row['requested_replay_url']
    history = []
    result = dict(requested_original_url=row['original_url'], requested_timestamp=row['timestamp'],
                  requested_replay_url=url, client_decoding='requests.iter_content(decode_unicode=False); HTTP content decoding enabled',
                  redirects=history, decoded_bytes=0)
    for hop in range(MAX_REDIRECTS + 1):
        timestamp, original = replay_identity(url)
        result.update(effective_replay_url=url, effective_timestamp=timestamp, effective_original_url=original)
        ceiling = LIMITS['max_response_decoded_bytes']
        if shutil.disk_usage(output).free < LIMITS['min_volume_free_bytes']:
            raise BudgetStop('storage_headroom')
        with db:
            charged = db.execute('SELECT COALESCE(SUM(charged),0) FROM hops').fetchone()[0]
            if charged + ceiling > LIMITS['max_tranche_decoded_bytes']:
                raise BudgetStop('decoded_budget')
            db.execute('INSERT INTO hops VALUES (?,?,?,?,NULL,NULL)', (attempt_id, hop, url, ceiling))
        atomic_write(output/'heartbeat.json', json.dumps(dict(attempt_id=attempt_id, hop=hop,
                     status='request_reserved', updated=time.time())).encode())
        while True:
            wait = db.execute('SELECT next_request FROM control').fetchone()[0] - time.time()
            if wait <= 0:
                break
            sleep(min(wait, 1))
        with db:
            db.execute('UPDATE control SET next_request=?', (time.time()+1,))
        response = session.get(url, stream=True, allow_redirects=False, timeout=(15, 15))
        deadline = time.monotonic() + 45
        body = bytearray()
        oversized = False
        try:
            # Stop at the ceiling without requesting one more decoded byte.
            chunk_size = min(8192, ceiling)
            chunks = iter(response.iter_content(chunk_size=chunk_size))
            while True:
                if time.monotonic() > deadline:
                    raise requests.Timeout('response elapsed deadline exceeded')
                if ceiling - len(body) < chunk_size:
                    oversized = True
                    break
                try:
                    chunk = next(chunks)
                except StopIteration:
                    break
                if not chunk:
                    continue
                if len(chunk) > ceiling - len(body):
                    oversized = True
                    break
                body.extend(chunk)
                if len(body) == ceiling:
                    oversized = True
                    break
            headers = dict(response.headers)
            status = response.status_code
        finally:
            response.close()
        raw = bytes(body)
        relative = f'raw/{attempt_id}-{hop}.gz'
        atomic_write(output / relative, gzip.compress(raw, mtime=0))
        evidence = dict(url=url, status_code=status, headers=headers, decoded_bytes=len(raw),
                        raw_path=relative, raw_sha256=hashlib.sha256(raw).hexdigest(), oversized=oversized)
        history.append(evidence)
        result['decoded_bytes'] += len(raw)
        with db:
            # Oversize and interrupted responses retain the full reservation.
            db.execute('UPDATE hops SET charged=?,decoded=?,result=? WHERE attempt_id=? AND hop=?',
                       (ceiling if oversized else len(raw), len(raw), json.dumps(evidence), attempt_id, hop))
        if oversized:
            return 'response_too_large', result
        if status in {301, 302, 303, 307, 308}:
            location = headers.get('Location') or headers.get('location')
            if not location:
                return 'redirect_missing_location', result
            next_url = urljoin(url, location)
            try:
                replay_identity(next_url)
            except ValueError as exc:
                result['blocked_redirect'] = next_url
                result['error'] = str(exc)
                return 'redirect_blocked', result
            if hop == MAX_REDIRECTS or any(h['url'] == next_url for h in history):
                return 'redirect_limit', result
            url = next_url
            continue
        if status in RETRYABLE or status >= 500:
            return 'retryable', result
        if status != 200:
            return 'terminal_http', result
        parser = TextParser()
        parser.feed(raw.decode(response.encoding or 'utf-8', errors='replace'))
        text = ' '.join(' '.join(parser.parts).split()).encode('utf-8')
        text_path = f'text/{attempt_id}.txt'
        atomic_write(output / text_path, text)
        result.update(text_path=text_path, text_sha256=hashlib.sha256(text).hexdigest(), text_bytes=len(text))
        if (timestamp, original) != (row['timestamp'], row['original_url']):
            return 'capture_mismatch', result
        content_type = headers.get('Content-Type', headers.get('content-type', '')).lower()
        if 'html' not in content_type:
            return 'non_html', result
        return ('fetched' if text else 'no_content'), result


def audit(db, rows, digest, output, stop_reason):
    expected = {r['proposed_capture_key'] for r in rows}
    current = list(db.execute('SELECT * FROM current ORDER BY key'))
    observed = {r['key'] for r in current}
    statuses = {}
    for row in current:
        statuses[row['status']] = statuses.get(row['status'], 0) + 1
    result = dict(queue_sha256=digest, expected_keys=len(expected), observed_keys=len(observed),
                  missing_keys=sorted(expected-observed), unexpected_keys=sorted(observed-expected),
                  statuses=statuses, attempts=db.execute('SELECT COUNT(*) FROM attempts').fetchone()[0],
                  duplicate_attempt_ids=0, http_requests=db.execute('SELECT COUNT(*) FROM hops').fetchone()[0],
                  last_update=time.time(), decoded_bytes=db.execute('SELECT COALESCE(SUM(decoded),0) FROM hops').fetchone()[0],
                  charged_decoded_bytes=db.execute('SELECT COALESCE(SUM(charged),0) FROM hops').fetchone()[0],
                  stored_bytes=sum(p.stat().st_size for folder in ('raw','text') for p in (output/folder).glob('*') if p.is_file()),
                  free_bytes=shutil.disk_usage(output).free, stop_reason=stop_reason,
                  acquisition_complete=bool(current) and all(r['status']=='fetched' for r in current),
                  current=[dict(r) for r in current])
    atomic_write(output/'audit.json', json.dumps(result, indent=2).encode())
    return result


def run_batch(queue, manifest, output_dir, max_new_attempts=None, session=None, sleep=time.sleep):
    rows, digest, queue_contract = load_queue(Path(queue), Path(manifest))
    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)
    if max_new_attempts is not None and max_new_attempts < 0:
        raise ValueError('max-new-attempts must be nonnegative')
    with (output/'writer.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        for folder in ('raw', 'text'):
            (output/folder).mkdir(exist_ok=True)
        ledger_contract = None if queue_contract['contract_version'] == LEGACY_CONTRACT_VERSION else queue_contract
        db = open_ledger(output/'ledger.sqlite3', rows, digest, ledger_contract)
        own_session = session is None
        session = session or requests.Session()
        if own_session:
            session.trust_env = False
            session.headers['User-Agent'] = 'candidate-website-research/1.0 bounded exact archive acquisition'
        lookup = {r['proposed_capture_key']: r for r in rows}
        stop = 'attempt_limit'
        started = 0
        try:
            while max_new_attempts is None or started < max_new_attempts:
                control = db.execute('SELECT * FROM control').fetchone()
                if control['circuit_until'] > time.time():
                    stop = 'circuit_open'
                    break
                current = db.execute("SELECT * FROM current WHERE status IN ('pending','retryable') AND attempts < ? AND next_due <= ? ORDER BY attempts,key LIMIT 1", (LIMITS['attempts_per_capture'],time.time())).fetchone()
                if not current:
                    due = db.execute("SELECT MIN(next_due) FROM current WHERE status IN ('pending','retryable') AND attempts < ?", (LIMITS['attempts_per_capture'],)).fetchone()[0]
                    if due is not None:
                        audit(db, rows, digest, output, 'retry_backoff')
                        sleep(min(30,max(0,due-time.time())))
                        continue
                    stop = 'queue_exhausted'
                    break
                if current['next_due'] > time.time():
                    stop = 'retry_backoff'
                    break
                if shutil.disk_usage(output).free < LIMITS['min_volume_free_bytes']:
                    stop = 'storage_headroom'
                    break
                charged = db.execute('SELECT COALESCE(SUM(charged),0) FROM hops').fetchone()[0]
                if charged + LIMITS['max_response_decoded_bytes'] > LIMITS['max_tranche_decoded_bytes']:
                    stop = 'decoded_budget'
                    break
                attempt_id = uuid.uuid4().hex
                with db:
                    db.execute("INSERT INTO attempts VALUES (?,?,?,NULL,'started',NULL)", (attempt_id,current['key'],time.time()))
                    db.execute("UPDATE current SET attempts=attempts+1,status='started' WHERE key=?", (current['key'],))
                started += 1
                audit(db, rows, digest, output, 'running')
                try:
                    status, evidence = fetch(db, session, lookup[current['key']], attempt_id, output, sleep)
                except BudgetStop as exc:
                    status, evidence = 'retryable', {'error': str(exc)}
                    stop = str(exc)
                except Exception as exc:
                    status, evidence = 'retryable', {'error': f'{type(exc).__name__}: {exc}'}
                with db:
                    db.execute('UPDATE attempts SET finished=?,status=?,result=? WHERE id=?', (time.time(),status,json.dumps(evidence),attempt_id))
                    delay = min(300, 30 * 2 ** current['attempts']) if status=='retryable' else 0
                    db.execute('UPDATE current SET status=?,next_due=?,result=? WHERE key=?', (status,time.time()+delay,json.dumps(evidence),current['key']))
                    failures = control['failures']+1 if status=='retryable' else 0
                    db.execute('UPDATE control SET failures=?,circuit_until=?', (failures,time.time()+300 if failures>=5 else 0))
                if stop in {'storage_headroom','decoded_budget'}:
                    break
                audit(db, rows, digest, output, 'running')
                sleep(1)
            return audit(db, rows, digest, output, stop)
        finally:
            db.close()
            if own_session:
                session.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--queue', type=Path, required=True)
    parser.add_argument('--manifest', type=Path, required=True)
    parser.add_argument('--output-dir', type=Path, required=True)
    parser.add_argument('--max-new-attempts', type=int)
    args = parser.parse_args()
    result = run_batch(args.queue,args.manifest,args.output_dir,args.max_new_attempts)
    print(json.dumps({k:v for k,v in result.items() if k != 'current'}, indent=2))


if __name__ == '__main__':
    main()
