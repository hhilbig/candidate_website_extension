import hashlib
import pytest
from scripts.audit_mayoral_priority_saved_evidence import relation, effective_original, verify_manifest

def test_priority_url_relation_preserves_path_query_and_candidate_join_boundary():
    assert relation('http://Example.com', 'http://example.com/') == 'exact_url'
    assert relation('http://example.com/', 'https://www.example.com/') == 'syntactic_scheme_www_variant_not_verified_equivalent'
    assert relation('http://example.com/A', 'http://example.com/a') == 'same_host_different_path_or_query'
    assert relation('http://example.com/?x=1', 'http://example.com/?x=2') == 'same_host_different_path_or_query'
    assert relation('http://example.com/', 'http://other.com/') == 'different_host_same_candidate_year'

def test_effective_archive_targets_can_change_host_or_path():
    assert relation('http://example.com/', effective_original('https://web.archive.org/web/20130101000000id_/http://other.com/')) == 'different_host_same_candidate_year'
    assert relation('http://example.com/', effective_original('https://wayback.archive-it.org/all/20130101000000id_/http://example.com/New?Case=X')) == 'same_host_different_path_or_query'
    assert effective_original('https://wayback.archive-it.org/all/20130101000000id_/http://Example.com/New?Case=X') == 'http://Example.com/New?Case=X'

def test_frozen_manifest_rejects_changed_input(tmp_path):
    path = tmp_path / 'input.csv'
    path.write_text('original')
    manifest = {'queue': {'path': 'input.csv', 'sha256': hashlib.sha256(path.read_bytes()).hexdigest()}, 'sources': []}
    verify_manifest(tmp_path, manifest)
    path.write_text('changed')
    with pytest.raises(ValueError, match='Frozen input changed'):
        verify_manifest(tmp_path, manifest)
