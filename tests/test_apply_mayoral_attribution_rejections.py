import copy
import pytest
from scripts.apply_mayoral_attribution_rejections import apply_rejections


def inputs():
    rows = [dict(candidate_url_id='a', candidate_year_id='y', exact_normalized_url='https://a.test/', ai_accepted_website='False'),
            dict(candidate_url_id='b', candidate_year_id='y', exact_normalized_url='https://b.test/', ai_accepted_website='False')]
    queue = [dict(candidate_url_id='a', candidate_year_id='y', evidence_sha256='hash',
                  exact_normalized_url='https://a.test/',
                  effective_timestamp='time', evidence_path='saved.gz')]
    packet = dict(input_sha256='queuehash', reviewer='AI reviewer', records=[
        dict(candidate_url_id='a', candidate_year_id='y', capture_sha256='hash',
             capture_timestamp='time', hash_verified=True,
             decision='reject_candidate_attribution', reason='Different candidate')])
    return rows, queue, packet


def test_wrong_candidate_excluded_and_original_preserved():
    rows, queue, packet = inputs()
    original = copy.deepcopy(rows)
    kept, rejected = apply_rejections(rows, queue, [('review.json', packet)], 'queuehash')
    assert rows == original and kept == [rows[1]]
    assert all(rejected[0][k] == v for k, v in rows[0].items())
    assert rejected[0]['attribution_exclusion_evidence_path'] == 'saved.gz'


@pytest.mark.parametrize('decision', ['reject_capture', 'reject_capture_wrong_office',
    'reject_capture_noncampaign', 'uncertain', 'support_capture'])
def test_capture_decisions_do_not_exclude_domain(decision):
    rows, queue, packet = inputs()
    packet['records'][0]['decision'] = decision
    kept, rejected = apply_rejections(rows, queue, [('r', packet)], 'queuehash')
    assert kept == rows and rejected == []


@pytest.mark.parametrize('field,value', [('candidate_year_id', 'wrong'),
    ('capture_sha256', 'wrong'), ('capture_timestamp', 'wrong'), ('hash_verified', False)])
def test_mismatched_evidence_fails(field, value):
    rows, queue, packet = inputs()
    packet['records'][0][field] = value
    with pytest.raises(ValueError):
        apply_rejections(rows, queue, [('r', packet)], 'queuehash')


def test_hash_duplicate_and_acceptance_conflict_fail():
    rows, queue, packet = inputs()
    with pytest.raises(ValueError):
        apply_rejections(rows, queue, [('r', packet)], 'wronghash')
    with pytest.raises(ValueError):
        apply_rejections(rows, queue, [('r', packet), ('s', packet)], 'queuehash')
    for field in ['human_accepted', 'ai_accepted_website']:
        changed = copy.deepcopy(rows)
        changed[0][field] = 'True'
        with pytest.raises(ValueError):
            apply_rejections(changed, queue, [('r', packet)], 'queuehash')


@pytest.mark.parametrize('url', ['https://different.test/', ''])
def test_changed_or_missing_target_url_fails(url):
    rows, queue, packet = inputs()
    rows[0]['exact_normalized_url'] = url
    with pytest.raises(ValueError, match='evidence mismatch'):
        apply_rejections(rows, queue, [('r', packet)], 'queuehash')
