"""Stage 3 page summary: window rule, text filter, and hashes of gzip-stored pages."""
import gzip
import hashlib

from scripts.summarize_mayoral_stage3_pages import summarize


def test_counts_pages_in_window_and_verifies_gzip_hash(tmp_path):
    html, text = b'<html>Vote Smith for Mayor</html>', b'Vote Smith for Mayor'
    (tmp_path / 'a.html.gz').write_bytes(gzip.compress(html))
    (tmp_path / 'a.txt').write_bytes(text)
    base = {'target_id': 't', 'fetch_status': 'fetched', 'raw_html_path': 'a.html.gz', 'text_path': 'a.txt',
            'raw_html_sha256': hashlib.sha256(html).hexdigest(), 'text_sha256': hashlib.sha256(text).hexdigest()}
    attempts = [dict(base, effective_timestamp='20200601000000', timestamp='', text_characters='20'),
                dict(base, effective_timestamp='20180101000000', timestamp='', text_characters='20'),
                dict(base, effective_timestamp='20200701000000', timestamp='', text_characters='0'),
                dict(base, fetch_status='no_content', effective_timestamp='20200601000000', timestamp='', text_characters='0')]
    target = {'target_id': 't', 'candidate_contest_id': 'c', 'registrable_domain': 'smith.com', 'election_date': '2020-11-03'}
    row = summarize([target], attempts, tmp_path)[0]
    assert (row['pages_fetched'], row['pages_with_text'], row['pages_with_text_in_window']) == (3, 2, 1)
    assert row['files_verified'] is True
    attempts[0]['raw_html_sha256'] = 'x'
    assert summarize([target], attempts, tmp_path)[0]['files_verified'] is False
