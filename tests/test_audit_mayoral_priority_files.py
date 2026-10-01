import gzip
import importlib.util
from pathlib import Path


spec = importlib.util.spec_from_file_location('priority_files', Path(__file__).parents[1]/'scripts/audit_mayoral_priority_files.py')
audit = importlib.util.module_from_spec(spec)
spec.loader.exec_module(audit)


def test_saved_latin1_html_matches_historical_text_hash(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    root = tmp_path/'data/municipal_discovery'
    root.mkdir(parents=True)
    raw = '<html>Mayoral caf\u00e9</html>'.encode('iso-8859-1')
    (root/'saved.html.gz').write_bytes(gzip.compress(raw))
    expected = audit.sha(raw.decode('iso-8859-1').encode('utf-8'))
    evidence = [{'source_url': 'http://example.org/', 'candidate_year_id': '1__2011',
                 'source_record': {'capture_content_sha256': expected}}]
    result = audit.scan(evidence)
    assert result['files_hashed'] == 1
    hit = result['matching_files'][0]
    assert 'stored_response_bytes' not in hit['hash_matches']
    assert hit['hash_matches']['response_text_iso-8859-1_to_utf8'] == expected
    assert hit['raw_sha256'] == audit.sha(raw)


def test_unrelated_content_does_not_match(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    root = tmp_path/'data/municipal_discovery'
    root.mkdir(parents=True)
    (root/'saved.html').write_bytes(b'<html>Other candidate</html>')
    evidence = [{'source_url': 'http://example.org/', 'candidate_year_id': '1__2011',
                 'source_record': {'capture_content_sha256': audit.sha(b'Expected content')}}]
    result = audit.scan(evidence)
    assert result['files_hashed'] == 1
    assert result['matching_files'] == []
