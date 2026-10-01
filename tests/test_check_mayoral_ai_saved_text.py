import gzip
import hashlib
import pytest
from scripts.check_mayoral_ai_saved_text import inspect_capture, select_records


def record(tmp_path, raw):
    path = tmp_path / 'capture.gz'
    path.write_bytes(gzip.compress(raw))
    return dict(capture_path=str(path), capture_sha256=hashlib.sha256(raw).hexdigest())


def test_extracts_visible_text_and_verifies_decompressed_hash(tmp_path):
    raw = b'<head><title>Hidden title</title></head><body><script>code</script><style>x</style><div hidden>hidden</div><p>Vote Jane for Mayor</p></body>'
    result = inspect_capture(record(tmp_path, raw))
    assert result['visible_text_excerpt'] == 'Vote Jane for Mayor'
    assert result['hash_verified'] is True
    assert result['saved_text_available'] is True
    assert result['persisted_extracted_text_available'] is None
    assert result['decompressed_bytes'] == len(raw)


def test_shell_is_not_saved_text(tmp_path):
    result = inspect_capture(record(tmp_path, b'<body><script>Campaign app</script></body>'))
    assert result['status'] == 'needs_review'
    assert result['saved_text_available'] is False


def test_nested_hidden_styles_do_not_crash(tmp_path):
    raw = b'<div style="display:none"><p style="color:red">Hidden</p></div><p>Vote Jane</p>'
    assert inspect_capture(record(tmp_path, raw))['visible_text_excerpt'] == 'Vote Jane'


def test_hash_mismatch_blocks_campaign_status(tmp_path):
    item = record(tmp_path, b'<p>Campaign content</p>')
    item['capture_sha256'] = 'wrong'
    result = inspect_capture(item)
    assert result['reason'] == 'hash_mismatch'
    assert result['saved_text_available'] is False


def test_missing_and_invalid_gzip_are_explicit(tmp_path):
    item = record(tmp_path, b'content')
    from pathlib import Path
    Path(item['capture_path']).write_bytes(b'invalid gzip')
    assert inspect_capture(item)['status'] == 'needs_review'
    Path(item['capture_path']).unlink()
    assert inspect_capture(item)['status'] == 'missing_file'


def test_selection_matches_capture_and_excludes_unsure():
    row = dict(candidate_url_id='a', candidate_year_id='y', ai_reviewed='True', ai_review_decision='accept',
               capture_content_sha256='hash', capture_effective_timestamp='20200101',
               capture_effective_url='https://archive.test/')
    review = dict(candidate_url_id='a', candidate_year_id='y', decision='accept',
                  capture_sha256='hash', capture_effective_timestamp='20200101',
                  capture_path='/saved.gz', name='Jane', rationale='Campaign')
    packet = dict(records=[review, dict(candidate_url_id='b', decision='unsure')])
    assert len(select_records([row], [packet])) == 1
    with pytest.raises(ValueError, match='Accepted review keys differ'):
        select_records([row], [dict(records=[])])
    with pytest.raises(ValueError, match='Duplicate review keys'):
        select_records([row], [packet, packet])
    review['capture_sha256'] = 'other capture'
    with pytest.raises(ValueError, match='evidence differs'):
        select_records([row], [packet])
