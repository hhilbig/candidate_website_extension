import csv
import gzip
import hashlib
import sqlite3
from scripts.export_mayoral_coverage_saved_evidence import exact, scan


def test_exact_preserves_relevant_url_parts():
    assert exact('https://web.archive.org/web/20200101000000id_/http://EXAMPLE.org/Campaign?A=b') == 'http://example.org/Campaign?A=b'
    assert exact('http://example.org/A') != exact('https://example.org/A')
    assert exact('http://www.example.org/A') != exact('http://example.org/A')
    assert exact('http://example.org/A') != exact('http://example.org/a')
    assert exact('https://user@example.org/') == ''


def test_scan_hashes_and_same_candidate_context(tmp_path):
    root = tmp_path / 'runs/production_all_inventory'
    root.mkdir(parents=True)
    raw = b'<html>saved response</html>'
    (root / 'raw.gz').write_bytes(gzip.compress(raw))
    (root / 'text.txt').write_bytes(b'saved response')
    evidence = dict(candidate_year_id='a', original_url='http://example.org/', timestamp='20200101000000',
                    capture_id='capture1', effective_replay_url='https://web.archive.org/web/20200102000000id_/http://example.org/',
                    effective_timestamp='20200102000000', fetch_status='fetched_timestamp_mismatch',
                    raw_html_path='raw.gz', text_path='text.txt', raw_html_sha256=hashlib.sha256(raw).hexdigest(),
                    text_sha256='wrong_hash')
    for name in ['capture_inventory.csv', 'fetch_attempts.csv']:
        with (root / name).open('w', newline='') as stream:
            writer = csv.DictWriter(stream, fieldnames=list(evidence))
            writer.writeheader()
            writer.writerow(evidence)
    result = scan({'volume':str(tmp_path), 'acquisition':str(tmp_path/'acquisition'), 'targets':[
        dict(candidate_year_id='a', candidate_url_id='target-a', exact_normalized_url='http://example.org/'),
        dict(candidate_year_id='b', candidate_url_id='target-b', exact_normalized_url='http://example.org/')
    ]})
    assert len(result['rows']) == 2
    assert {r['candidate_url_id'] for r in result['rows']} == {'target-a'}
    saved = next(r for r in result['rows'] if r['evidence_kind']=='saved_response')
    assert saved['raw_saved']
    assert not saved['text_saved']
    assert saved['capture_timestamp']=='20200102000000'
    assert 'text:hash_mismatch' in saved['hash_status']


def test_targeted_inventory_keeps_metadata_without_claiming_download(tmp_path):
    root = tmp_path / 'inventory_22_20260914/run'
    root.mkdir(parents=True)
    db = sqlite3.connect(root / 'ledger.sqlite3')
    db.executescript('''
        CREATE TABLE queries (query_key TEXT, candidate_year_id TEXT, status TEXT);
        CREATE TABLE capture_rows (row_id INTEGER, query_key TEXT, original TEXT, timestamp TEXT, accepted INTEGER);
        INSERT INTO queries VALUES ('q','a','COMPLETE');
        INSERT INTO queries VALUES ('empty','b','EMPTY');
        INSERT INTO capture_rows VALUES (1,'q','http://example.org/','20200101000000',1);
        INSERT INTO capture_rows VALUES (2,'q','https://example.org/','20200102000000',1);
    ''')
    db.commit()
    db.close()
    result = scan({'volume':str(tmp_path), 'acquisition':str(tmp_path/'acquisition'), 'targets':[
        dict(candidate_year_id='a', candidate_url_id='target-a', exact_normalized_url='http://example.org/')
    ]})
    assert len(result['rows']) == 1
    row = result['rows'][0]
    assert row['evidence_kind'] == 'targeted_inventory'
    assert row['capture_timestamp'] == '20200101000000'
    assert not row['raw_saved'] and not row['text_saved']
    audit = result['source_audit'][-1]
    assert audit['rows'] == 2 and audit['matched_evidence_rows'] == 1
    assert audit['query_status_counts'] == {'COMPLETE':1,'EMPTY':1}
