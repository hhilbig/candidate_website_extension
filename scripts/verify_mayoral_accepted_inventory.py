"""Verify a stopped metadata-only inventory against its frozen inputs and bytes."""
import argparse
import collections
import gzip
import hashlib
import json
from pathlib import Path
import sqlite3
import time

try:
    from . import inventory_mayoral_accepted_urls as inventory
except ImportError:
    import inventory_mayoral_accepted_urls as inventory


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def verify(queue, run):
    rows, queue_hash = inventory.load_queue(queue)
    sources = {r['query_key']: r for r in rows}
    db = sqlite3.connect(f'file:{run.resolve()}/ledger.sqlite3?mode=ro', uri=True)
    db.row_factory = sqlite3.Row
    assert db.execute('PRAGMA quick_check').fetchone()[0] == 'ok'
    contract = json.loads(db.execute('SELECT value FROM contract WHERE id=1').fetchone()[0])
    assert contract['queue_sha256'] == queue_hash
    assert contract['endpoint'] == inventory.ENDPOINT
    assert contract['limits'] == inventory.LIMITS
    attempts = list(db.execute('SELECT * FROM attempts'))
    queries = list(db.execute('SELECT * FROM queries ORDER BY queue_index'))
    assert {r['query_key'] for r in queries} == set(sources)
    assert len(attempts) == len({r['attempt_id'] for r in attempts})
    assert all(r['status'] != 'STARTED' and r['finished'] is not None for r in attempts)
    assert len(attempts) <= inventory.LIMITS['max_http_requests']
    assert sum(r['charged_bytes'] for r in attempts) <= inventory.LIMITS['max_total_decoded_bytes']
    raw_checked = 0
    for attempt in attempts:
        params = json.loads(attempt['params'])
        source = sources[attempt['query_key']]
        assert params == inventory.request_params(source, attempt['resume_key'])
        assert 'collapse' not in params and 'filter' not in params
        assert 0 <= attempt['charged_bytes'] <= inventory.LIMITS['max_response_decoded_bytes']
        if not attempt['raw_path']:
            continue
        raw = gzip.decompress((run / attempt['raw_path']).read_bytes())
        assert len(raw) == attempt['decoded_bytes'] <= attempt['charged_bytes']
        assert hashlib.sha256(raw).hexdigest() == attempt['raw_sha256']
        raw_checked += 1
        if attempt['status'] not in {'CONTINUE', 'COMPLETE', 'EMPTY'}:
            continue
        parsed, continuation, _ = inventory.parse_cdx_plaintext(raw, source)
        stored = list(db.execute('SELECT * FROM capture_rows WHERE attempt_id=? ORDER BY row_index',
                                 (attempt['attempt_id'],)))
        assert [tuple(row[f] for f in inventory.FIELDS) for row in stored] == parsed
        assert all(row['accepted'] == 1 for row in stored)
        page = db.execute('SELECT * FROM pages WHERE attempt_id=?', (attempt['attempt_id'],)).fetchone()
        assert page['row_count'] == len(parsed) and page['accepted'] == 1
        assert page['output_resume_key'] == continuation
        assert attempt['http_status'] == 200
    per_query = []
    for query in queries:
        pages = list(db.execute('SELECT * FROM pages WHERE query_key=? AND accepted=1 ORDER BY page_index',
                                (query['query_key'],)))
        assert [p['page_index'] for p in pages] == list(range(len(pages)))
        previous_token = None
        for page in pages:
            assert page['input_resume_key'] == previous_token
            previous_token = page['output_resume_key']
        if query['status'] in {'COMPLETE', 'EMPTY'}:
            assert pages and previous_token is None
            assert pages[-1]['status'] == query['status']
        records = list(db.execute('SELECT * FROM capture_rows WHERE query_key=? AND accepted=1',
                                  (query['query_key'],)))
        assert len(records) == query['accepted_rows'] == sum(p['row_count'] for p in pages)
        assert (query['status'] != 'EMPTY') or not records
        assert (query['status'] != 'COMPLETE') or records
        source = sources[query['query_key']]
        per_query.append(dict(candidate_url_id=query['candidate_url_id'],
            candidate_year_id=query['candidate_year_id'], full_name=source.get('full_name', ''),
            requested_url=query['requested_url'], from_timestamp=query['from_timestamp'],
            to_timestamp=query['to_timestamp'], status=query['status'], rows=len(records),
            distinct_timestamps=len({r['timestamp'] for r in records}),
            distinct_original_urls=len({r['original'] for r in records}),
            status_codes=dict(collections.Counter(r['statuscode'] for r in records)),
            mime_types=dict(collections.Counter(r['mimetype'] for r in records)),
            html_200_rows=sum(r['statuscode'] == '200' and r['mimetype'] == 'text/html' for r in records),
            attempts=query['total_attempts'], accepted_pages=len(pages)))
    db.close()
    return dict(verified_at_utc=time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
        queue_sha256=queue_hash, runner_sha256=sha(Path(inventory.__file__)),
        ledger_sha256=sha(run / 'ledger.sqlite3'), expected_queries=len(rows),
        statuses=dict(collections.Counter(q['status'] for q in queries)),
        all_queries_terminal=all(q['status'] in {'COMPLETE', 'EMPTY'} for q in queries),
        attempts=len(attempts), raw_response_hashes_verified=raw_checked,
        accepted_cdx_rows=sum(q['rows'] for q in per_query),
        charged_decoded_bytes=sum(r['charged_bytes'] for r in attempts),
        decoded_bytes=sum(r['decoded_bytes'] or 0 for r in attempts),
        run_file_bytes=sum(p.stat().st_size for p in run.rglob('*') if p.is_file()),
        replay_requests=0, query_results=per_query,
        scope='All returned CDX rows for the exact-match URLs and recorded windows. No digest collapse or status/MIME filter. Metadata does not establish page acquisition, ownership, or full-host history.')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--queue', type=Path, required=True)
    parser.add_argument('--run-dir', type=Path, required=True)
    args = parser.parse_args()
    result = verify(args.queue, args.run_dir)
    with (args.run_dir / 'verification.json').open('x') as stream:
        json.dump(result, stream, indent=2)
    print(json.dumps({k: v for k, v in result.items() if k != 'query_results'}, indent=2))
