import copy
import pytest
from scripts.integrate_mayoral_ai_review import integrate


def inputs():
    source = [dict(candidate_url_id='a', candidate_year_id='y', raw_url='url', human_accepted='False'),
              dict(candidate_url_id='b', candidate_year_id='z', raw_url='other', human_accepted='True')]
    validation = [dict(candidate_url_id='a', candidate_year_id='y', raw_url='url',
                       ai_reviewed='True', ai_review_model='gpt-5.6-sol', ai_review_date='2026-09-14',
                       ai_review_decision='accept', ai_review_reason='Evidence',
                       capture_content_sha256='hash', capture_effective_timestamp='date', capture_effective_url='archive')]
    reviews = [dict(candidate_url_id='a', candidate_year_id='y', url='url', capture_sha256='hash',
                    capture_effective_timestamp='date', decision='accept', hash_verified=True,
                    capture_path='remote', record_file='review.json')]
    return source, validation, reviews


def test_complete_table_preserved_and_evidence_attached():
    source, validation, reviews = inputs()
    old = copy.deepcopy(source)
    out = integrate(source, validation, reviews)
    assert len(out) == 2 and source == old
    assert all(all(r[k] == v for k, v in s.items()) for s, r in zip(source, out))
    assert out[0]['ai_accepted_website'] == 'True'
    assert out[0]['ai_review_evidence_sha256'] == 'hash'
    assert out[1]['ai_reviewed'] == 'False'


def test_unsure_does_not_count_as_accepted():
    source, validation, reviews = inputs()
    validation[0]['ai_review_decision'] = reviews[0]['decision'] = 'unsure'
    assert integrate(source, validation, reviews)[0]['ai_accepted_website'] == 'False'


@pytest.mark.parametrize('field,value', [('candidate_year_id', 'wrong'), ('url', 'wrong'),
    ('capture_sha256', 'wrong'), ('capture_effective_timestamp', 'wrong'), ('hash_verified', False)])
def test_evidence_mismatch_fails(field, value):
    source, validation, reviews = inputs()
    reviews[0][field] = value
    with pytest.raises(ValueError):
        integrate(source, validation, reviews)


def test_missing_duplicate_and_column_collision_fail():
    source, validation, reviews = inputs()
    for bad in [[], reviews * 2]:
        with pytest.raises(ValueError):
            integrate(source, validation, bad)
    source[0]['ai_reviewed'] = 'True'
    with pytest.raises(ValueError):
        integrate(source, validation, reviews)
