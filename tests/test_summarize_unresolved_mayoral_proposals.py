import pytest
from scripts.summarize_unresolved_mayoral_proposals import summarize


def test_preserves_unknown_and_supported_distinction():
    rows = [dict(candidate_url_id='a', historical_campaign_content_supported='False', final_validation_status='no_archive_capture'),
            dict(candidate_url_id='b', historical_campaign_content_supported='True')]
    result = summarize(rows)
    assert len(result) == 1
    assert 'not_archive_absence' in result[0]['next_action']
    assert result[0]['new_archive_request_authorized'] == 'False'
    assert 'next_action' not in rows[0]


def test_unknown_status_and_duplicate_keys_fail():
    with pytest.raises(ValueError):
        summarize([dict(candidate_url_id='a', final_validation_status='new')])
    with pytest.raises(ValueError):
        summarize([dict(candidate_url_id='a'), dict(candidate_url_id='a')])


def test_outside_window_and_sensitivity_are_separate():
    result = summarize([dict(candidate_url_id='a', final_validation_status='outside_headline_and_sensitivity_windows'),
                        dict(candidate_url_id='b', final_validation_status='outside_headline_365_within_sensitivity')])
    assert result[0]['next_action'] != result[1]['next_action']
