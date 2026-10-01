import copy
import csv
import importlib.util
import json
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location('attach', ROOT / 'scripts/attach_mayoral_capture_reviews.py')
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)
BASE = ROOT / 'quality_reports/mayoral_ai_review_2026-09-14'


@pytest.fixture
def inputs():
    with (BASE / 'validation.csv').open(newline='') as handle:
        rows = list(csv.DictReader(handle))
    packets = [(BASE / f'flagged_capture_review_{part}.json',
                json.loads((BASE / f'flagged_capture_review_{part}.json').read_text()))
               for part in ('a', 'b')]
    return rows, packets, MODULE.digest(BASE / 'validation.csv')


def test_preserves_all_values_and_separates_capture_decisions(inputs):
    rows, packets, sha = inputs
    original = copy.deepcopy(rows)
    result = MODULE.attach(rows, packets, sha)
    assert rows == original
    assert len(result) == 120
    reviewed = [r for r in result if r['ai_capture_reviewed'] == 'True']
    assert len(reviewed) == 30
    assert {r['ai_capture_review_decision'] for r in reviewed} == {'support_capture'}
    for old, new in zip(rows, result):
        assert {k: new[k] for k in old} == old
        if new['ai_capture_reviewed'] == 'True':
            assert new['ai_reviewed'].lower() != 'true'
            assert old['flag_resolution_status'] in json.loads(new['ai_capture_review_remaining_flags'])
        else:
            assert all(new[field] == '' for field in MODULE.FIELDS)


@pytest.mark.parametrize('failure', [
    'duplicate_source', 'duplicate_review', 'missing_review', 'unexpected_review',
    'input_hash', 'candidate_year', 'contest', 'evidence_hash', 'evidence_date',
    'mixed_pair', 'hash_false', 'hash_string', 'hash_absent', 'observed_hash',
    'lost_flag', 'invalid_model', 'invalid_decision', 'overwrite',
])
def test_fail_closed(inputs, failure):
    rows, packets, sha = inputs
    review = packets[0][1]['reviews'][0]
    row = next(r for r in rows if r['candidate_url_id'] == review['candidate_url_id'])
    if failure == 'duplicate_source':
        rows.append(copy.deepcopy(rows[0]))
    elif failure == 'duplicate_review':
        packets[0][1]['reviews'].append(copy.deepcopy(review))
    elif failure == 'missing_review':
        packets[0][1]['reviews'].pop()
    elif failure == 'unexpected_review':
        review['candidate_url_id'] = 'unexpected'
    elif failure == 'input_hash':
        packets[0][1]['input_sha256'] = 'wrong'
    elif failure == 'candidate_year':
        review['candidate_year_id'] = 'wrong'
    elif failure == 'contest':
        review['candidate_contest_id'] = 'wrong'
    elif failure == 'evidence_hash':
        review['evidence_sha256'] = 'wrong'
    elif failure == 'evidence_date':
        review['evidence_timestamp'] = '19000101000000'
    elif failure == 'mixed_pair':
        row['supplement_raw_html_sha256'] = 'supplementhash'
        row['supplement_effective_timestamp'] = '19000101000000'
        review['evidence_timestamp'] = row['supplement_effective_timestamp']
    elif failure in {'hash_false', 'hash_string', 'hash_absent'}:
        if failure == 'hash_absent':
            del review['decoded_sha256_verified']
        else:
            review['decoded_sha256_verified'] = False if failure == 'hash_false' else 'true'
    elif failure == 'observed_hash':
        review['observed_decoded_sha256'] = 'wrong'
    elif failure == 'lost_flag':
        review['remaining_flags'].remove(row['flag_resolution_status'])
    elif failure == 'invalid_model':
        packets[0][1]['model'] = 'other'
    elif failure == 'invalid_decision':
        review['decision'] = 'accept'
    elif failure == 'overwrite':
        rows[0]['ai_capture_reviewed'] = 'True'
    with pytest.raises(ValueError):
        MODULE.attach(rows, packets, sha)
