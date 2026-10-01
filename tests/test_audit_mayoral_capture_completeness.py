import json

from scripts.audit_mayoral_capture_completeness import relation, summarize


REQUEST = {'candidate_url_id': 'u', 'candidate_year_id': 'c', 'full_name': 'Name',
           'exact_normalized_url': 'https://example.com/A?x=1', 'registrable_domain': 'example.com'}
TARGET = {'batch': 'b', 'target_id': 't', 'candidate_year_id': 'c',
          'registrable_domain': 'example.com', 'scrape_window_start': '2010-01-01',
          'scrape_window_end': '2011-01-01', 'url_variants_json': json.dumps(['https://example.com/'])}
QUERY = {'batch': 'b', 'target_id': 't', 'variant_url': 'https://example.com/',
         'window_start': '20100101', 'window_end': '20110101', 'inventory_status': 'found',
         'raw_hash_verified': True, 'raw_data_rows': 20}


def test_absence_from_old_targets_is_unknown():
    audit, retry, queries = summarize([REQUEST], {'targets': [], 'queries': [], 'captures': []})
    assert audit[0]['inventory_state'] == 'not_in_saved_target_inventories'
    assert queries[0]['expected_capture_count'] == 'unknown_before_inventory'
    assert not retry


def test_query_cap_and_hash_are_completion_requirements():
    remote = {'targets': [TARGET], 'queries': [QUERY], 'captures': []}
    assert summarize([REQUEST], remote)[0][0]['inventory_state'] == 'saved_filtered_queries_terminal'
    for change in [{'raw_data_rows': 50000}, {'raw_hash_verified': False}, {'inventory_status': 'request_error'}]:
        remote['queries'] = [QUERY | change]
        assert summarize([REQUEST], remote)[0][0]['query_gap_keys'] == 1


def test_failed_later_batch_preserves_prior_verified_capture():
    capture = {'candidate_year_id': 'c', 'timestamp': '20100601000000',
               'original_url': REQUEST['exact_normalized_url'], 'registrable_domain': 'example.com',
               'inventory_sources': ['a', 'b'], 'queue_sources': ['a', 'b'],
               'fetches': [{'raw_verified': True, 'fetch_status': 'fetched', 'raw_html_bytes': 12, 'compressed_bytes': 8},
                           {'raw_verified': False, 'fetch_status': 'request_error'}]}
    audit, retry, _ = summarize([REQUEST], {'targets': [TARGET], 'queries': [QUERY], 'captures': [capture]})
    assert audit[0]['exact_verified_saved'] == 1
    assert not retry


def test_url_relations_preserve_path_query_and_host():
    assert relation('https://example.com/A?x=1', 'http://www.example.com/A?x=1') == 'scheme_www_variant'
    assert relation('https://example.com/A?x=1', 'https://example.com/a?x=1') == 'domain_other_path_or_host'
    assert relation('https://example.com/A?x=1', 'https://example.com/A?x=2') == 'domain_other_path_or_host'
    assert relation('https://example.com/A', 'https://other.example.com/A') == 'domain_other_path_or_host'
    assert relation('https://example.com/A', 'http://www.example.com:80/A') == 'scheme_www_variant'
    assert relation('https://example.com/A', 'https://example.com:80/A') == 'domain_other_path_or_host'


def test_saved_no_content_is_not_missing_acquisition():
    capture = {'candidate_year_id': 'c', 'timestamp': '20100601000000',
               'original_url': REQUEST['exact_normalized_url'], 'registrable_domain': 'example.com',
               'inventory_sources': ['a'], 'queue_sources': ['a'],
               'fetches': [{'raw_verified': True, 'fetch_status': 'no_content', 'raw_html_bytes': 0, 'compressed_bytes': 20}]}
    audit, retry, _ = summarize([REQUEST], {'targets': [TARGET], 'queries': [QUERY], 'captures': [capture]})
    assert audit[0]['exact_verified_saved'] == 1
    assert audit[0]['exact_saved_no_content'] == 1
    assert audit[0]['exact_unsaved'] == 0
    assert not retry
