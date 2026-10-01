import pytest
from scripts.prepare_mayoral_remaining_inventory import prepare


def inputs():
    a=[dict(candidate_url_id='a',candidate_year_id='c',exact_normalized_url='https://example.org/',
        full_name='Example',inventory_windows_json='[["2020-01-01","2021-01-01"]]')]
    return a,[dict(a[0],ai_accepted_website='true')]


def test_preserves_window_and_exact_url():
    a,c=inputs()
    r=prepare(a,[],c)
    assert r[0]['proposed_window_start']=='2020-01-01'
    assert r[0]['proposed_window_end']=='2021-01-01'
    assert r[0]['exact_normalized_url']=='https://example.org/'
    assert prepare(a,[{'candidate_url_id':'a'}],c)==[]


def test_ambiguous_window_does_not_invent_bounds():
    a,c=inputs()
    a[0]['inventory_windows_json']='[]'
    with pytest.raises(ValueError):prepare(a,[],c)


def test_changed_url_or_universe_rejected():
    a,c=inputs()
    c[0]['exact_normalized_url']='http://example.org/'
    with pytest.raises(ValueError):prepare(a,[],c)
    with pytest.raises(ValueError):prepare([],[],c)
