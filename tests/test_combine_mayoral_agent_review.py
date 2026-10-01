import pytest
from scripts.combine_mayoral_agent_review import combine


def test_review_is_separate_and_preserves_inputs():
    rows = [dict(candidate_url_id='a', candidate_year_id='y', strict_coverage_eligible_result='True', human_accepted='False'),
            dict(candidate_url_id='b', candidate_year_id='z', strict_coverage_eligible_result='False', human_accepted='False')]
    reviews = [dict(candidate_url_id='a', candidate_year_id='y', recommendation='accept')]
    result = combine(rows, reviews)
    assert result['human_approvals_added'] == 0
    assert result['recommendations'] == {'accept': 1}
    assert result['input_proposals'] == 2
    assert all(r['human_accepted'] == 'False' for r in rows)
    for bad in [[], reviews * 2, [dict(candidate_url_id='b', candidate_year_id='z', recommendation='accept')]]:
        with pytest.raises(ValueError):
            combine(rows, bad)
    with pytest.raises(ValueError):
        combine(rows, [dict(candidate_url_id='a', candidate_year_id='z', recommendation='accept')])
    with pytest.raises(ValueError):
        combine(rows, [dict(candidate_url_id='a', candidate_year_id='y', recommendation='human_approved')])
