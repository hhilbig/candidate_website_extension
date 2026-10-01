import pytest
from scripts.attach_mayoral_discovery_capture_reviews import attach


def inputs():
    row=dict(candidate_url_id='a',candidate_year_id='y',exact_normalized_url='url',ai_accepted_website='True')
    review=dict(candidate_url_id='a',candidate_year_id='y',exact_normalized_url='url',ai_capture_reviewed='True',
                ai_capture_review_decision='support_capture',evidence_path='p',evidence_sha256='h',effective_timestamp='t',effective_original_url='u')
    return row,review


def test_attach_preserves_acceptance_and_unreviewed_rows():
    row,review=inputs();other={**row,'candidate_url_id':'b'}
    out=attach([row,other],[],[review])
    assert all(out[0][k]==v for k,v in row.items())
    assert out[0]['ai_capture_evidence_sha256']=='h'
    assert out[1]['ai_capture_reviewed']=='False'
    assert out[1]['ai_accepted_website']=='True'


def test_unapplied_rejection_fails_but_reference_is_allowed():
    row,review=inputs();review['ai_capture_review_decision']='reject_candidate_attribution'
    with pytest.raises(ValueError,match='Unapplied'):attach([row],[],[review])
    assert attach([],[row],[review])==[]


@pytest.mark.parametrize('field,value',[('candidate_year_id','other'),('exact_normalized_url','other'),('ai_capture_reviewed','False')])
def test_changed_identity_fails(field,value):
    row,review=inputs();review[field]=value
    with pytest.raises(ValueError):attach([row],[],[review])


def test_duplicate_unknown_and_collision_fail():
    row,review=inputs()
    with pytest.raises(ValueError):attach([row],[],[review,review])
    with pytest.raises(ValueError):attach([],[],[review])
    row['ai_capture_reviewed']='False'
    with pytest.raises(ValueError):attach([row],[],[review])
