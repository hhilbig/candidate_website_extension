"""Keep possible HTML captures without storing weekly asset metadata copies."""
import gzip
import hashlib
import json
from types import SimpleNamespace
from urllib.parse import parse_qs, urlsplit

import pytest

from src.collection_2026 import (
    CAPTURE_IDENTITY_VERSION, CDX_ASSET_MIME_FILTER, Store, available_captures,
    capture_omission_reason, inventory, stable,
)


CANDIDATE = {'candidate_id': 'TEST1', 'website_url': 'https://example.test/'}
EVENT = {'event_id': 'primary', 'date': '2026-06-02', 'timezones': ['America/New_York']}
HEADER = ['timestamp', 'original', 'statuscode', 'mimetype']


def capture(url='https://example.test/', mime='text/html', status='200', **extra):
    return dict(timestamp='20260601120000', original=url, statuscode=status, mimetype=mime, **extra)


def body(rows, resume=''):
    data = [HEADER, *[[row[key] for key in HEADER] for row in rows]]
    if resume:
        data.extend([[], [resume]])
    return json.dumps(data).encode()


def saved_client(store, responses):
    queue = iter(responses)
    requests = []
    def get(url, key, archive=False):
        requests.append(url)
        response = next(queue)
        aid = store.attempt(key, url)
        if isinstance(response, bytes):
            path, digest = store.blob(response)
            store.finish_attempt(aid, 'ok', {'body_path': path, 'body_sha256': digest})
            return {'status': 'ok', 'body': response, 'body_path': path}
        store.finish_attempt(aid, 'request_error', {})
        return {'status': 'request_error', 'body': b''}
    return SimpleNamespace(get=get, requests=requests)


@pytest.mark.parametrize('url,mime', [
    ('https://example.test/photo.png', 'text/html'),
    ('https://example.test/file%2EJPG?download=1', 'warc/revisit'),
    ('https://example.test/main.css', '-'),
    ('https://example.test/data.json', 'application/octet-stream'),
    ('https://example.test/opaque', 'image/png'),
    ('https://example.test/opaque', 'font/woff2'),
    ('https://example.test/opaque', 'application/pdf'),
    ('https://example.test/opaque', 'text/plain'),
])
def test_known_assets_are_omitted_without_fetches(url, mime):
    assert capture_omission_reason(capture(url, mime))


@pytest.mark.parametrize('mime,status', [
    ('text/html', '200'), ('application/xhtml+xml', '200'), ('warc/revisit', '200'),
    ('warc/revisit', '-'), ('unk', '200'), ('-', '200'), ('application/octet-stream', '200'),
    ('text/plain', '301'), ('image/png', '302'), ('application/json', '307'),
])
def test_html_unknown_and_nonasset_redirects_stay_available(tmp_path, mime, status):
    store = Store(tmp_path)
    assert capture_omission_reason(capture(mime=mime, status=status)) == ''
    store.capture('reviewed-import', capture(mime=mime, status=status, candidate_id='TEST1'))
    store.db.commit()
    assert len(available_captures(store, ['reviewed-import'], EVENT, 'pre')) == 1
    store.db.close()


def test_repeated_weekly_inventory_reuses_captures_and_preserves_raw_audits(tmp_path):
    store = Store(tmp_path)
    records = [capture(), capture('https://example.test/logo.png', 'image/png'),
               capture('https://example.test/about', 'text/plain', '301')]
    raw = body(records)
    client = saved_client(store, [raw, raw])
    first = inventory(store, client, CANDIDATE, CANDIDATE['website_url'], '20260914')
    second = inventory(store, client, CANDIDATE, CANDIDATE['website_url'], '20260922')
    assert first != second
    saved = store.rows('captures')
    assert len(saved) == 2
    assert all(row['inventory_key'] == first for row in saved)
    assert all(row['latest_complete_inventory_key'] == second for row in saved)
    assert len(available_captures(store, [second], EVENT, 'pre')) == 2
    assert len(available_captures(store, [first], EVENT, 'pre')) == 2
    result = json.loads(store.db.execute('SELECT result FROM work WHERE key=?', (second,)).fetchone()[0])
    assert result['capture_rows'] == result['returned_cdx_rows'] == 3
    assert result['eligible_capture_rows'] == result['reused_capture_rows'] == 2
    assert result['new_unique_capture_rows'] == 0
    assert result['omitted_metadata_rows'] == 1
    assert result['omitted_by_reason'] == {'known_asset_url': 1}
    assert CDX_ASSET_MIME_FILTER is None
    assert result['server_filter'] is None
    for request in client.requests:
        params = parse_qs(urlsplit(request).query)
        assert 'filter' not in params
        assert params['url'] == ['example.test/']
        assert params['matchType'] == ['prefix']
    attempts = store.rows('attempts')
    assert len(attempts) == 2
    for attempt in attempts:
        assert gzip.decompress((tmp_path / attempt['detail']['body_path']).read_bytes()) == raw
    store.db.close()


def test_capture_first_seen_in_partial_query_requires_completed_evidence(tmp_path):
    store = Store(tmp_path)
    initial = capture()
    next_page = capture('https://example.test/about', 'warc/revisit')
    client = saved_client(store, [body([initial], 'resume'), None, body([initial, next_page])])
    partial = inventory(store, client, CANDIDATE, CANDIDATE['website_url'], '20260914')
    assert available_captures(store, [partial], EVENT, 'pre') == []
    complete = inventory(store, client, CANDIDATE, CANDIDATE['website_url'], '20260922')
    assert len(available_captures(store, [complete], EVENT, 'pre')) == 2
    original = next(row for row in store.rows('captures') if row['original'] == initial['original'])
    assert original['inventory_key'] == partial
    assert original['latest_complete_inventory_key'] == complete
    assert len(store.rows('captures')) == 2
    store.db.close()


def test_dedup_keeps_distinct_candidate_ids_and_capture_timestamps(tmp_path):
    store = Store(tmp_path)
    for key, cid, stamp in [('one', 'TEST1', '20260601120000'), ('two', 'TEST1', '20260601120000'),
                            ('three', 'TEST2', '20260601120000'), ('four', 'TEST1', '20260601120100')]:
        row = capture(candidate_id=cid)
        row['timestamp'] = stamp
        store.capture(key, row)
    store.db.commit()
    assert len(store.rows('captures')) == 3
    assert all(row['capture_identity_version'] == CAPTURE_IDENTITY_VERSION for row in store.rows('captures'))
    store.db.close()


def insert_legacy(store, key, row):
    cid = stable(key, row['timestamp'], row['original'])
    row = dict(row, capture_id=cid, inventory_key=key)
    store.db.execute('INSERT INTO captures VALUES(?,?,?)', (cid, key, json.dumps(row)))
    return cid


def test_migration_counts_all_rows_preserves_first_provenance_and_raw_files(tmp_path):
    store = Store(tmp_path)
    for key, cid, state in [('old', 'TEST1', 'retryable'), ('new', 'TEST1', 'complete'), ('other', 'TEST2', 'complete')]:
        store.add_work(key, 'cdx_inventory', cid, {'prefix': 'example.test/', 'through_date': '20260914'})
        store.update(key, state)
    first_id = insert_legacy(store, 'old', capture(candidate_id='TEST1', source_note='first'))
    insert_legacy(store, 'new', capture(candidate_id='TEST1', source_note='later'))
    insert_legacy(store, 'new', capture('https://example.test/logo.png', 'image/png', candidate_id='TEST1'))
    insert_legacy(store, 'new', capture('https://example.test/data', 'application/json', candidate_id='TEST1'))
    insert_legacy(store, 'new', capture('https://example.test/redirect', 'image/png', '302', candidate_id='TEST1'))
    insert_legacy(store, 'new', capture('https://example.test/revisit', 'warc/revisit', '-', candidate_id='TEST1'))
    insert_legacy(store, 'other', capture(candidate_id='TEST2'))
    store.db.commit()
    raw = body([capture(), capture('https://example.test/logo.png', 'image/png')])
    raw_path, digest = store.blob(raw)
    aid = store.attempt('old', 'https://web.archive.org/cdx/search/cdx?url=example.test')
    store.finish_attempt(aid, 'ok', {'body_path': raw_path, 'body_sha256': digest})
    store.page({'page_id': 'page', 'candidate_id': 'TEST1'})
    before_rows = store.rows('captures')
    untouched = {table: store.rows(table) for table in ('work', 'attempts', 'pages')}
    file_hash = hashlib.sha256((tmp_path / raw_path).read_bytes()).hexdigest()
    preview = store.migrate_capture_inventory(dry_run=True)
    assert preview['before_capture_rows'] == 7
    assert preview['after_capture_rows'] == 4
    assert preview['omitted_metadata_rows'] == 2
    assert preview['duplicate_inventory_rows'] == 1
    assert store.rows('captures') == before_rows
    report = store.migrate_capture_inventory()
    assert report['before_capture_rows'] == 7 and report['after_capture_rows'] == 4
    assert report['raw_response_files_modified'] == 0
    saved = store.rows('captures')
    original = next(row for row in saved if row.get('candidate_id') == 'TEST1' and row['original'] == 'https://example.test/')
    assert original['inventory_key'] == 'old'
    assert original['original_capture_id'] == first_id
    assert original['source_note'] == 'first'
    assert original['latest_complete_inventory_key'] == 'new'
    assert len(available_captures(store, ['new'], EVENT, 'pre')) == 3
    assert hashlib.sha256((tmp_path / raw_path).read_bytes()).hexdigest() == file_hash
    assert gzip.decompress((tmp_path / raw_path).read_bytes()) == raw
    for table in untouched:
        assert store.rows(table) == untouched[table]
    repeated = store.migrate_capture_inventory()
    assert repeated['before_capture_rows'] == repeated['after_capture_rows'] == 4
    assert repeated['duplicate_inventory_rows'] == repeated['omitted_metadata_rows'] == 0
    assert len(list((tmp_path / 'migrations').glob('*.json'))) == 2
    store.db.close()


def test_migration_rolls_back_on_invalid_metadata(tmp_path):
    store = Store(tmp_path)
    store.db.execute('INSERT INTO captures VALUES(?,?,?)', ('bad', 'inventory', json.dumps({'mimetype': 'text/html'})))
    store.db.commit()
    with pytest.raises(KeyError):
        store.migrate_capture_inventory()
    assert store.db.execute('SELECT COUNT(*) FROM captures').fetchone()[0] == 1
    assert not store.db.execute("SELECT name FROM sqlite_master WHERE name='captures_migration_v2'").fetchall()
    assert not (tmp_path / 'migrations').exists()
    store.db.close()
