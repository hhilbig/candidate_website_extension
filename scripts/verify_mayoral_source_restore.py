"""Verify a restored source archive before a new writer starts."""
import argparse
import collections
import csv
import gzip
import hashlib
import json
from pathlib import Path
import sqlite3


def digest(path, compressed=False):
    result = hashlib.sha256()
    opener = gzip.open if compressed else open
    with opener(path, 'rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            result.update(block)
    return result.hexdigest()


def verify(root):
    manifest = json.loads((root / 'ARCHIVE_MANIFEST.json').read_text())
    for name, info in manifest['files'].items():
        path = root / name
        assert path.stat().st_size == info['bytes'], name
        assert digest(path) == info['sha256'], name
    run = root / 'work/production'
    db = sqlite3.connect(f'file:{run}/sources.sqlite?mode=ro', uri=True)
    assert db.execute('PRAGMA integrity_check').fetchone()[0] == 'ok'
    counts = {name: db.execute(f'SELECT COUNT(*) FROM {name}').fetchone()[0]
              for name in ('sources', 'contexts', 'attempts', 'current', 'candidates', 'claims')}
    assert counts['sources'] == 58463 and counts['contexts'] == 85049
    assert counts['attempts'] == 14042 and counts['current'] == 11900
    assert counts['candidates'] == 4817
    assert db.execute('SELECT COUNT(*) FROM attempts WHERE status="started"').fetchone()[0] == 0
    checks = collections.Counter()
    seen = set()
    for (encoded,) in db.execute('SELECT result_json FROM current'):
        record = json.loads(encoded)
        for field in ('raw_path', 'parsed_path', 'text_path'):
            name = record.get(field)
            if not name or name in seen:
                continue
            seen.add(name)
            path = run / name
            assert path.is_file(), name
            expected = record.get('raw_sha256') if field == 'raw_path' else path.name.split('.')[0]
            assert digest(path, compressed=True) == expected, name
            checks[field] += 1
    with (run / 'combined/candidate_roster.csv').open() as stream:
        roster = list(csv.DictReader(stream))
    assert len(roster) == len({r['candidate_year_id'] for r in roster}) == 4817
    source_audit = json.loads((run / 'audit.json').read_text())
    assert source_audit['combined_roster']['active_candidate_years_with_url_leads'] == 1093
    result = {'archive_member_hashes_verified': len(manifest['files']), 'database_integrity': 'ok',
              'table_counts': counts, 'unique_content_hashes_verified': dict(checks),
              'baseline_url_candidate_years': 1093, 'baseline_attempts': 14042,
              'baseline_downloaded_raw_bytes': source_audit['downloaded_raw_bytes'],
              'database_sha256': digest(run / 'sources.sqlite'),
              'archive_manifest_sha256': digest(root / 'ARCHIVE_MANIFEST.json')}
    print(json.dumps(result, indent=2))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('root', type=Path)
    verify(parser.parse_args().root)
