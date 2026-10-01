import pytest
from scripts.prepare_mayoral_capture_proposal import prepare


def row():
    return dict(candidate_year_id='a', timestamp='20200101120000', original_url='https://example.com/',
                url_relation='exact', reason='not_queued')


def test_selection_retains_deferred_without_input_mutation():
    a = row()
    b = {**a, 'timestamp': '20200102120000', 'url_relation': 'scheme_www_variant'}
    selected, deferred = prepare([a, b])
    assert len(selected) == 1 and deferred == [b]
    assert 'proposed_capture_key' not in a
    assert selected[0]['requested_replay_url'].startswith('https://web.archive.org/web/20200101120000id_/')
    assert selected == prepare([b, a])[0]


@pytest.mark.parametrize('change', [{'reason': 'no_content'}, {'timestamp': 'bad'},
                                   {'original_url': 'https://info@example.com/'},
                                   {'original_url': 'file:///tmp/a'}])
def test_rejects_unsafe_or_wrong_targets(change):
    with pytest.raises(ValueError):
        prepare([{**row(), **change}])


def test_duplicate_keys_fail():
    with pytest.raises(ValueError):
        prepare([row(), row()])
