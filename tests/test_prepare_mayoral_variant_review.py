import copy
import json
import unittest

from scripts.prepare_mayoral_variant_review import prepare


class PrepareVariantReviewTests(unittest.TestCase):
    def setUp(self):
        self.candidate = dict(candidate_url_id='url1', candidate_year_id='person__2011',
                              candidate_contest_id='contest1', full_name='Name', geo_name='City',
                              state_abb='CA', year='2011', month='4', exact_normalized_url='http://a/')
        self.evidence = dict(raw_path='/remote/a.html.gz', raw_sha256='a' * 64,
                             effective_timestamp='20110413031009',
                             effective_original_url='http://www.a/', requested_original_url='http://www.a/',
                             election_reference_date='2011-04-15', relation='saved_syntactic_variant_only',
                             raw_hash_check='verified', days_absolute=2, within_365_days=True)
        self.audit = dict(candidate_url_id='url1', candidate_year_id='person__2011',
                          exact_normalized_url='http://a/', classification='saved_syntactic_variant_only')
        self.validation = dict(candidate_url_id='url1', candidate_year_id='person__2011',
                               capture_content_sha256='b' * 64, capture_effective_timestamp='20110413031009',
                               supplement_raw_html_sha256='a' * 64, supplement_effective_timestamp='20110414000000')

    def run_prepare(self, validation=None):
        audit = {**self.audit, 'evidence_json': json.dumps({self.evidence['relation']: self.evidence})}
        return prepare([audit], validation or [], [self.candidate])

    def test_joint_pair_not_independent_hash_timestamp(self):
        queue, overlap = self.run_prepare([self.validation])
        self.assertEqual((len(queue), len(overlap)), (1, 0))
        self.validation['supplement_effective_timestamp'] = self.evidence['effective_timestamp']
        queue, overlap = self.run_prepare([self.validation])
        self.assertEqual((len(queue), len(overlap)), (0, 1))
        self.assertEqual(overlap[0]['validation_match_bases'], ['supplement'])

    def test_outside_window_retained(self):
        self.evidence.update(effective_timestamp='20100413031009', days_absolute=367, within_365_days=False)
        queue, _ = self.run_prepare()
        self.assertEqual(queue[0]['review_flags'], ['outside_365_day_window'])

    def test_identity_conflict_rejected(self):
        self.candidate['candidate_year_id'] = 'other__2011'
        with self.assertRaisesRegex(ValueError, 'Conflicting joined field'):
            self.run_prepare()

    def test_empty_evidence_rejected(self):
        self.evidence['raw_path'] = ''
        with self.assertRaisesRegex(ValueError, 'Missing saved evidence'):
            self.run_prepare()

    def test_duplicate_key_rejected(self):
        with self.assertRaisesRegex(ValueError, 'Duplicate or empty'):
            prepare([], [], [self.candidate, copy.deepcopy(self.candidate)])

    def test_dates_checked(self):
        self.evidence['days_absolute'] = 1
        with self.assertRaisesRegex(ValueError, 'date offset mismatch'):
            self.run_prepare()


if __name__ == '__main__':
    unittest.main()
