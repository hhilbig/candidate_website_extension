import copy
import pytest
from scripts.summarize_mayoral_variant_reviews import summarize, review_rows


def inputs():
    q=[dict(candidate_url_id='a',candidate_year_id='y',evidence_sha256='h',effective_timestamp='t'),
       dict(candidate_url_id='b',candidate_year_id='z',evidence_sha256='i',effective_timestamp='u')]
    p=dict(input_sha256='queue',reviewer='AI',model='model',acceptance_changes=0,records=[
        dict(candidate_url_id='a',candidate_year_id='y',capture_sha256='h',capture_timestamp='t',
             hash_verified=True,decision='reject_capture',reason='parking',remaining_flags=['parking'])])
    return q,p


def test_reports_partial_coverage_not_approval():
    q,p=inputs(); old=copy.deepcopy((q,p));r=summarize(q,[('r',p)],'queue')
    assert (q,p)==old
    assert r['remaining_rows']==1 and r['unreviewed_keys']==['b']
    assert r['decision_counts']=={'capture_rejected':1}
    assert r['attribution_rejection_keys']==[] and r['acceptance_changes']==0


def test_attribution_rejection_separate():
    q,p=inputs();p['records'][0]['decision']='reject_candidate_attribution'
    assert summarize(q,[('r',p)],'queue')['attribution_rejection_keys']==['a']


def test_complete_mode_rejects_partial_and_accepts_exact_coverage():
    q,p=inputs()
    with pytest.raises(ValueError, match='Incomplete review coverage'):
        summarize(q,[('r',p)],'queue',require_complete=True)
    r=summarize(q[:1],[('r',p)],'queue',require_complete=True)
    assert r['remaining_rows']==0 and r['reviewed_rows']==r['queue_rows']==1


def test_row_export_preserves_queue_and_distinguishes_missing_review():
    q,p=inputs();r=summarize(q,[('r',p)],'queue');rows=review_rows(q,r)
    assert all(all(row[k]==v for k,v in original.items()) for row,original in zip(rows,q))
    assert rows[0]['ai_capture_reviewed'] is True
    assert rows[0]['ai_capture_review_decision']=='reject_capture'
    assert rows[1]['ai_capture_reviewed'] is False
    assert rows[1]['ai_capture_review_decision']==''
    assert 'ai_accepted_website' not in rows[0]
    q[0]['ai_capture_reviewed']=False
    with pytest.raises(ValueError,match='overwrite'):
        review_rows(q,r)


@pytest.mark.parametrize('field,value', [('candidate_year_id','z'),('capture_sha256','i'),
    ('capture_timestamp','u'),('hash_verified',False),('decision','approve'),('reason',''),('remaining_flags','parking')])
def test_bad_record_fails(field,value):
    q,p=inputs();p['records'][0][field]=value
    with pytest.raises(ValueError):summarize(q,[('r',p)],'queue')


def test_duplicate_unknown_and_stale_hash_fail():
    q,p=inputs()
    with pytest.raises(ValueError):summarize(q,[('r',p),('s',p)],'queue')
    with pytest.raises(ValueError):summarize(q,[('r',p)],'old')
    p['records'][0]['candidate_url_id']='unknown'
    with pytest.raises(ValueError):summarize(q,[('r',p)],'queue')
