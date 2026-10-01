import json
import pytest

from scripts.prepare_mayoral_inventory_capture_proposal import prepare


def row(**changes):
    return dict(dict(row_id=1, candidate_year_id='c', candidate_url_id='u',
        timestamp='20200101000000', original='http://www.example.org:80/',
        requested_url='https://example.org/', saved_exact_response=False,
        statuscode='200', mimetype='text/html'), **changes)


def test_variants_and_duplicates_keep_original_provenance():
    queue, decisions = prepare([row(), row(row_id=2)])
    assert len(queue)==1 and len(decisions)==2
    assert queue[0]['url_relation']=='index_returned_url_variant'
    assert queue[0]['original_url']=='http://www.example.org:80/'
    assert json.loads(queue[0]['inventory_row_ids_json'])==[1,2]


def test_revisits_selected_and_errors_preserved_separately():
    queue, decisions = prepare([row(mimetype='warc/revisit',statuscode='-'),
        row(row_id=2,statuscode='301'),row(row_id=3,saved_exact_response=True)])
    assert len(queue)==1 and queue[0]['selection_basis']=='propose_revisit_capture'
    assert len(decisions)==3
    assert decisions[1]['disposition']=='retain_redirect_or_error_metadata_for_separate_review'
    assert decisions[2]['disposition']=='verified_saved_response'


def test_duplicate_ids_rejected():
    with pytest.raises(ValueError):
        prepare([row(),row()])
