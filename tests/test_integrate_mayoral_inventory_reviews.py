import pytest
from scripts.integrate_mayoral_inventory_reviews import integrate


def fixture(decision):
    row=dict(row_id=1,query_key='q',candidate_url_id='u',candidate_year_id='c',timestamp='t',
        original='https://example.org/',mimetype='text/html',statuscode='200',duplicate_of_row_id='',
        saved_exact_response=True,exact_evidence=[dict(raw_verified=True,raw_sha256='raw')])
    review=dict(row_ids=[1],candidate_url_id='u',candidate_year_id='c',capture_timestamp='t',
        capture_url='https://example.org/',raw_sha256='raw',text_sha256='text',usable_static_text=True,
        decision=decision,reason='evidence')
    review['capture_bindings']=[dict(row_id=1,candidate_url_id='u',candidate_year_id='c',
        capture_timestamp='t',capture_url='https://example.org/',date_validity_decision='support')]
    return [row],dict(reviews_by_raw_sha256={'raw':review})


@pytest.mark.parametrize('decision,eligible',[('support',True),('caveat',False),('reject_capture',False),('uncertain',False)])
def test_unsupported_capture_cannot_increase_text_coverage(decision,eligible):
    rows,packet=fixture(decision)
    result=integrate(rows,packet)
    assert len(result)==1 and result[0]['ai_supported_mayoral_text']==eligible


def test_wrong_capture_or_missing_review_rejected():
    rows,packet=fixture('support')
    packet['reviews_by_raw_sha256']['raw']['capture_bindings'][0]['capture_timestamp']='wrong'
    with pytest.raises(ValueError):integrate(rows,packet)
    with pytest.raises(ValueError):integrate(rows,{'reviews_by_raw_sha256':{}})


def test_identical_bytes_do_not_replace_actual_timestamps():
    rows,packet=fixture('support')
    rows.append(dict(rows[0],row_id=2,timestamp='later'))
    review=packet['reviews_by_raw_sha256']['raw']
    review['row_ids']=[1,2]
    with pytest.raises(ValueError):integrate(rows,packet)
    review['capture_bindings'].append(dict(review['capture_bindings'][0],row_id=2,capture_timestamp='later'))
    result=integrate(rows,packet)
    assert [r['timestamp'] for r in result]==['t','later']


def test_date_uncertainty_cannot_increase_supported_text_coverage():
    rows,packet=fixture('support')
    packet['reviews_by_raw_sha256']['raw']['capture_bindings'][0]['date_validity_decision']='caveat'
    assert not integrate(rows,packet)[0]['ai_supported_mayoral_text']
