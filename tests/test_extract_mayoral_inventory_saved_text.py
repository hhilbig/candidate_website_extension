import gzip
import json
import pytest

import scripts.extract_mayoral_inventory_saved_text as module
from scripts.extract_mayoral_inventory_saved_text import extract


def test_excludes_nonvisible_content_and_does_not_execute_scripts():
    text,_=extract(b'<head><title>Title</title></head><p>Vote Jane</p><script>throw 1</script>'
        b'<div hidden>hidden</div><span style="display:none">secret</span><iframe src="https://example.org">frame</iframe>')
    assert text=='Vote Jane'


def test_decodes_declared_non_utf8():
    text,_=extract(b'<meta charset="windows-1252"><p>Jos\xe9</p>')
    assert text=='Jos\u00e9'


def fixture_payload(tmp_path, monkeypatch, bad_hash=False):
    monkeypatch.setattr(module.shutil, 'disk_usage', lambda _: type('Usage', (), {'free': 9_000_000_000})())
    raw=b'<p>Campaign text</p>'
    path=tmp_path/'raw.gz'
    path.write_bytes(gzip.compress(raw))
    evidence=dict(raw_path=str(path),raw_sha256='bad' if bad_hash else module.digest(raw))
    rows=[dict(row_id=i,candidate_url_id='u',candidate_year_id='c',timestamp='20200101000000',
        original='https://example.org/',saved_exact_response=True,exact_evidence=[evidence]) for i in (1,2)]
    source=tmp_path/'source.json'
    source.write_text(json.dumps(rows))
    return dict(source=str(source),source_sha256=module.digest(source.read_bytes()),
        output=str(tmp_path/'text'),script_sha256='test')


def test_persisted_text_and_every_row_verified(tmp_path,monkeypatch):
    payload=fixture_payload(tmp_path,monkeypatch)
    result=module.run_remote(payload)
    assert result['selected_inventory_rows']==result['observed_inventory_rows']==2
    assert result['unique_raw_files']==result['nonempty_text_files']==1
    assert {r['row_id'] for r in result['records']}=={1,2}
    for row in result['records']:
        text=gzip.decompress(module.Path(row['text_path']).read_bytes())
        assert text==b'Campaign text' and module.digest(text)==row['text_sha256']
    with pytest.raises(ValueError):
        module.run_remote(payload)


def test_bad_raw_hash_creates_no_outputs(tmp_path,monkeypatch):
    payload=fixture_payload(tmp_path,monkeypatch,bad_hash=True)
    with pytest.raises(ValueError):
        module.run_remote(payload)
    assert not module.Path(payload['output']).exists()
