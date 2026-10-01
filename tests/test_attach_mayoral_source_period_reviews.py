import copy
import pytest
from scripts.attach_mayoral_source_period_reviews import attach


def fixture():
    rows=[dict(candidate_url_id='u',candidate_year_id='c',raw_url='https://example.org',
        source_expansion_claim_ids_json='["claim"]',ai_accepted_website='False')]
    review=dict(candidate_url_id='u',row_key={'candidate_year_id':'c'},raw_url='https://example.org',
        source_claim_ids=['claim'],overall_judgment='retain_pending_period_evidence',
        office_year_period_support={'reason':'Period unknown'},source_ownership={'status':'supported'},
        archive_validation={'status':'not_performed'})
    return rows,dict(reviews_by_candidate_url_id={'u':review},selection={'selected_rows':1},
        requested_review_model='gpt-5.6-sol')


def test_preserves_approval_and_every_input_cell():
    rows,packet=fixture();old=copy.deepcopy(rows)
    result=attach(rows,packet,'hash')
    assert rows==old
    assert all(result[0][k]==v for k,v in old[0].items())
    assert result[0]['source_period_ai_decision']=='retain_pending_period_evidence'
    assert result[0]['ai_accepted_website']=='False'


@pytest.mark.parametrize('field,value',[('source_claim_ids',['wrong']),('raw_url','https://other.org')])
def test_wrong_binding_rejected(field,value):
    rows,packet=fixture();packet['reviews_by_candidate_url_id']['u'][field]=value
    with pytest.raises(ValueError):attach(rows,packet,'hash')
