import pytest

from scripts.reconcile_mayoral_inventory_saved import match_records, replay


def record(**changes):
    return dict(dict(row_id=1, query_key='q', candidate_year_id='c', urlkey='org,example)/',
        timestamp='20200101000000', original='https://example.org/', mimetype='text/html',
        statuscode='200', digest='ABC', length='10'), **changes)


def evidence(**changes):
    return dict(dict(timestamp='20200101000000', original_url='https://example.org/',
        provider='web.archive.org', raw_verified=True, text_verified=True,
        candidate_year_id='c'), **changes)


@pytest.mark.parametrize('changes', [
    {'original_url': 'http://example.org/'}, {'original_url': 'https://www.example.org/'},
    {'provider': 'wayback.archive-it.org'}, {'raw_verified': False},
    {'timestamp': '20200102000000'}, {'candidate_year_id': 'other'},
    {'original_url': 'https://example.org/article'},
])
def test_non_exact_or_unverified_never_counts(changes):
    result = match_records([record()], [evidence(**changes)])[0]
    assert not result['saved_exact_response']
    assert not result['saved_exact_text']


def test_duplicate_and_non_html_rows_preserved():
    rows = [record(statuscode='301'), record(row_id=2, statuscode='301')]
    result = match_records(rows, [evidence(text_verified=False)])
    assert len(result) == 2
    assert result[1]['duplicate_of_row_id'] == 1
    assert all(r['saved_exact_response'] and not r['saved_exact_text'] for r in result)


def test_provider_and_query_preserved():
    assert replay('https://wayback.archive-it.org/123/20200101000000id_/https://example.org/?a=1') == (
        'wayback.archive-it.org', '20200101000000', 'https://example.org/?a=1')
    assert replay('https://example.org/20200101000000/https://example.org/') is None


def test_url_time_only_binding_allowed_but_does_not_invent_candidate():
    result = match_records([record()], [evidence(candidate_year_id='')])[0]
    assert result['saved_exact_response']
    assert result['exact_evidence'][0]['candidate_year_id'] == ''
