import copy
import pytest
from scripts.label_mayoral_ai_review import attach


def inputs():
    rows = [dict(candidate_url_id='a', candidate_year_id='y', raw_url='https://a.test/',
                 capture_content_sha256='hash', capture_effective_timestamp='20110101',
                 human_accepted='False', strict_coverage_eligible_result='True'),
            dict(candidate_url_id='b', strict_coverage_eligible_result='False', human_accepted='False')]
    packet = dict(model='gpt-5.6-sol', review_type='ai_review', input_sha256='input', records=[
        dict(candidate_url_id='a', candidate_year_id='y', url='https://a.test/',
             capture_sha256='hash', capture_effective_timestamp='20110101',
             decision='accept', hash_verified=True, rationale='Campaign evidence')])
    return rows, packet


def test_preserves_every_source_field_and_marks_only_reviewed():
    rows, packet = inputs()
    original = copy.deepcopy(rows)
    output, counts = attach(rows, [packet], 'input')
    assert rows == original
    assert counts == {'accept': 1}
    for old, new in zip(rows, output):
        assert all(new[k] == v for k, v in old.items())
    assert output[0]['ai_reviewed'] == 'True'
    assert output[1]['ai_reviewed'] == 'False'
    assert all(r['human_accepted'] == 'False' for r in output)


@pytest.mark.parametrize('field,value', [('candidate_year_id', 'wrong'), ('url', 'wrong'),
    ('capture_sha256', 'wrong'), ('capture_effective_timestamp', 'wrong'),
    ('decision', 'human_approved'), ('hash_verified', False), ('rationale', '')])
def test_rejects_invalid_evidence(field, value):
    rows, packet = inputs()
    packet['records'][0][field] = value
    with pytest.raises(ValueError):
        attach(rows, [packet], 'input')


def test_rejects_duplicate_missing_model_and_hash():
    rows, packet = inputs()
    for packets in [[], [packet, packet], [{**packet, 'model': 'unknown'}],
                    [{**packet, 'input_sha256': 'wrong'}]]:
        with pytest.raises(ValueError):
            attach(rows, packets, 'input')


def test_no_column_overwrite():
    rows, packet = inputs()
    rows[0]['ai_reviewed'] = 'existing'
    with pytest.raises(ValueError):
        attach(rows, [packet], 'input')
