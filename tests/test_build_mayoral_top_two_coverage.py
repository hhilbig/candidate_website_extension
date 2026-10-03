"""Test factual result-role imports with synthetic cities and source records."""
from copy import deepcopy

import pytest

from scripts.build_mayoral_top_two_coverage import (
    build, evidence_for, exact_url, period_match, truth, recorded_period_metadata, combine_result_sources,
    merge_coverage_gaps,
)


def test_result_supersession_is_explicit_and_preserves_old_record():
    *_, record = fixture()
    old = dict(record, verification_status='unresolved')
    with pytest.raises(ValueError, match='explicit supersession'):
        combine_result_sources([[old], [record]])
    record['supersedes_verification_status'] = 'unresolved'
    assert combine_result_sources([[old], [record]]) == ([record], [old])
    record['supersedes_verification_status'] = 'verified_decisive'
    with pytest.raises(ValueError, match='correction review'):
        combine_result_sources([[record], [record]])


def test_calendar_completion_does_not_hide_missing_result():
    cities, roster, urls, _ = fixture()
    audit = {'city_id': '0000001', 'year': 2020, 'status': 'complete',
             'source_url': 'https://example.test/calendar', 'expected_election_dates': ['2020-11-03']}
    result = build(cities, roster, urls, [], city_year_overrides=[audit])
    assert any(r['verification_status'] == 'calendar_result_missing' for r in result[2])
    assert not result[4]['full_universe_complete']
    audit['expected_election_dates'] = ['2021-11-03']
    with pytest.raises(ValueError, match='outside audited year'):
        build(cities, roster, urls, [], city_year_overrides=[audit])


def test_decisive_primary_needs_separate_source():
    cities, roster, urls, result = fixture()
    result['stage'] = 'primary_nominating'
    with pytest.raises(ValueError, match='decisive stage'):
        build(cities, roster, urls, [result])
    result['decisive_stage_source_url'] = 'https://example.test/elected-without-runoff'
    assert build(cities, roster, urls, [result])[4]['contested_elections'] == 1


def test_complete_calendar_cannot_omit_verified_election():
    cities, roster, urls, result = fixture()
    audit = {'city_id': '0000001', 'year': 2020, 'status': 'complete',
             'source_url': 'https://example.test/calendar', 'expected_election_dates': ['2020-06-02']}
    with pytest.raises(ValueError, match='omits a verified election'):
        build(cities, roster, urls, [result], city_year_overrides=[audit])
    result['candidates'] = result['candidates'][:1]
    audit['status'] = 'no_election'
    audit['expected_election_dates'] = []
    with pytest.raises(ValueError, match='conflicts with verified election'):
        build(cities, roster, urls, [result], city_year_overrides=[audit])


def test_earlier_round_only_calendar_preserves_distinction():
    cities, _, _, _ = fixture()
    audit = {'city_id': '0000001', 'year': 2020, 'status': 'earlier_round_only',
             'source_url': 'https://example.test/calendar', 'expected_election_dates': []}
    result = build(cities, [], [], [], city_year_overrides=[audit])
    row = next(r for r in result[1] if r['year'] == 2020)
    assert row['status'] == 'earlier_round_only'
    assert row['audit_complete'] is True


def fixture():
    cities = [{'city_id': '0000001', 'city_name': 'Fixture city', 'state_abb': 'ZZ', 'roster_place_fips': '0000001'}]
    roster = [{'candidate_year_id': 'legacy-a', 'place_fips': '0000001', 'year': '2020',
               'month': '11', 'full_name': 'Example A', 'contest': 'contest-a'}]
    urls = [{'candidate_url_id': 'url-a', 'candidate_year_id': 'legacy-a', 'month': '11',
             'exact_normalized_url': 'https://example.test/Campaign', 'url_type': 'custom_domain'}]
    result = {'city_id': '0000001', 'city_name': 'Fixture city', 'state_abb': 'ZZ',
              'election_date': '2020-11-03', 'stage': 'general', 'election_type': 'regular',
              'source_url': 'https://example.test/official-results', 'verification_status': 'verified_decisive',
              'candidates': [{'name': 'Example A', 'role': 'winner', 'votes': 5},
                             {'name': 'Example B', 'role': 'runner_up', 'votes': 7}]}
    return cities, roster, urls, result


@pytest.mark.parametrize('value, expected', [('True', True), ('true', True), ('YES', True),
                                            ('False', False), ('0', False), ('unknown', None), ('', None), (None, None)])
def test_boolean_normalization(value, expected):
    assert truth(value) is expected


def test_bad_boolean_rejected():
    with pytest.raises(ValueError):
        truth('possibly')


def test_recorded_metadata_remains_visible_without_attribution():
    _, _, urls, _ = fixture()
    saved = {('url-a', 'legacy-a'): [{'capture_original_url': 'https://example.test/Campaign',
                                    'capture_timestamp': '20201001000000'}]}
    assert recorded_period_metadata(urls, '2020-11-03', saved, {}, {}) is True
    assert evidence_for(urls, '2020-11-03', {}, {}, saved)[0] is None


def test_unconfirmed_certification_cannot_be_promoted():
    cities, roster, urls, result = fixture()
    result['certification_status'] = 'not_confirmed'
    with pytest.raises(ValueError, match='certification'):
        build(cities, roster, urls, [result])


def test_saved_raw_and_text_need_hashes_and_attribution():
    _, _, urls, _ = fixture()
    saved = {'exact_normalized_url': 'https://example.test/Campaign',
             'capture_original_url': 'https://example.test/Campaign', 'capture_timestamp': '20201001000000',
             'raw_saved': 'True', 'text_saved': 'true', 'source_ref': 'fixture-ledger',
             'raw_path': 'raw.gz', 'text_path': 'text.gz', 'raw_sha256': 'a' * 64, 'text_sha256': 'b' * 64}
    index = {('url-a', 'legacy-a'): [saved, saved]}
    assert evidence_for(urls, '2020-11-03', {}, {}, index)[1] is None
    urls[0]['human_accepted'] = 'True'
    result = evidence_for(urls, '2020-11-03', {}, {}, index)
    assert result[:2] == (True, True)
    assert len(result[3]) == 1
    saved['text_sha256'] = ''
    assert evidence_for(urls, '2020-11-03', {}, {}, index)[1] is None


def test_saved_inventory_without_text_still_counts_for_discovery():
    _, _, urls, _ = fixture()
    urls[0]['human_accepted'] = 'True'
    saved = {'exact_normalized_url': 'https://example.test/Campaign',
             'capture_original_url': 'https://example.test/Campaign', 'capture_timestamp': '20201001000000',
             'raw_saved': 'False', 'text_saved': 'False', 'source_ref': 'fixture-inventory'}
    assert evidence_for(urls, '2020-11-03', {}, {}, {('url-a', 'legacy-a'): [saved]})[:2] == (True, None)


def test_invalid_timestamp_and_invalid_port():
    assert not period_match('20201103999999', '2020-11-03')
    assert not period_match('20201103garbage', '2020-11-03')
    assert exact_url('https://example.test:bad/path') == ''
    assert exact_url('https://[invalid/path') == ''


def test_exact_url_preserves_case_path_query_and_scheme():
    assert exact_url('HTTPS://EXAMPLE.TEST/Campaign?Year=2020#top') == 'https://example.test/Campaign?Year=2020'
    assert exact_url('http://example.test/campaign') != exact_url('https://example.test/Campaign')
    assert exact_url('https://web.archive.org/web/20201101000000id_/https://EXAMPLE.TEST/Campaign') == 'https://example.test/Campaign'
    assert exact_url('https://user@example.test/') == ''


def test_window_uses_actual_date_and_inclusive_365_days():
    assert period_match('20191104000000', '2020-11-03')
    assert not period_match('20191103000000', '2020-11-03')
    assert period_match('20211103000000', '2020-11-03')
    assert not period_match('20211104000000', '2020-11-03')
    assert not period_match('invalid', '2020-11-03')


def test_imports_supplied_official_roles_without_inferring_from_votes():
    cities, roster, urls, result = fixture()
    _, _, _, candidates, summary, _ = build(cities, roster, urls, [result])
    assert [c['official_result_role'] for c in candidates] == ['winner', 'runner_up']
    assert candidates[0]['official_votes'] == 5
    assert candidates[1]['official_votes'] == 7
    assert candidates[0]['candidate_year_id'] == 'legacy-a'
    assert candidates[0]['url_lead'] is True
    assert candidates[1]['candidate_year_id'] == ''
    assert summary['full_universe_complete'] is False


@pytest.mark.parametrize('stage', ['general', 'runoff', 'ranked_choice', 'special'])
def test_decisive_stage_roles_import(stage):
    cities, roster, urls, result = fixture()
    result['stage'] = stage
    assert all(r['eligible'] for r in build(cities, roster, urls, [result])[3])


def test_unverified_and_uncontested_records_are_not_contested_denominator():
    cities, roster, urls, result = fixture()
    result['verification_status'] = 'uncertified'
    assert build(cities, roster, urls, [result])[4]['candidate_records'] == 0
    result['verification_status'] = 'verified_decisive'
    result['candidates'] = result['candidates'][:1]
    output = build(cities, roster, urls, [result])
    assert output[2][0]['verification_status'] == 'uncontested'
    assert output[4]['candidate_records'] == 0


def test_unresolved_roles_not_inferred():
    cities, roster, urls, result = fixture()
    result['candidates'][1]['role'] = 'unresolved_tie'
    output = build(cities, roster, urls, [result])
    assert output[2][0]['verification_status'] == 'unresolved_candidate_roles'
    assert output[4]['candidate_records'] == 0


def test_date_cutoff_and_special_election_ids():
    cities, roster, urls, result = fixture()
    special = deepcopy(result)
    special['election_type'] = 'special'
    special['candidates'] = [{'name': 'Example C', 'role': 'winner'}, {'name': 'Example D', 'role': 'runner_up'}]
    outside = deepcopy(result)
    outside['election_date'] = '2026-09-15'
    output = build(cities, roster, urls, [result, special, outside])
    assert len(output[2]) == 2
    assert len({r['election_id'] for r in output[2]}) == 2
    assert len(output[5]) == 1


def test_prior_round_stays_unresolved_without_explicit_reconciliation():
    cities, roster, urls, result = fixture()
    roster.append(dict(roster[0], candidate_year_id='primary-a', contest='primary', month='6'))
    output = build(cities, roster, urls, [result])
    assert any(r['election_id'] == 'legacy__primary' for r in output[2])


def test_duplicate_input_keys_and_official_elections_fail():
    cities, roster, urls, result = fixture()
    for args in [(cities * 2, roster, urls, [result]), (cities, roster * 2, urls, [result]),
                 (cities, roster, urls * 2, [result]), (cities, roster, urls, [result, result])]:
        with pytest.raises(ValueError, match='duplicate'):
            build(*args)


def test_capture_duplicate_evidence_does_not_duplicate_references():
    _, _, urls, _ = fixture()
    review = {'effective_original_url': 'https://example.test/Campaign', 'effective_timestamp': '20201001000000',
              'ai_capture_review_decision': 'support_capture'}
    reviews = {('url-a', 'legacy-a'): [review, review]}
    archive, downloaded, substantive, refs = evidence_for(urls, '2020-11-03', reviews, {})
    assert archive is True and substantive is True
    assert downloaded is None
    assert len(refs) == 1


@pytest.mark.parametrize('original', ['https://example.test/campaign', 'http://example.test/Campaign',
                                    'https://www.example.test/Campaign', 'https://example.test/Campaign?other=1'])
def test_review_requires_exact_url(original):
    _, _, urls, _ = fixture()
    review = {'effective_original_url': original, 'effective_timestamp': '20201001000000',
              'ai_capture_review_decision': 'support_capture'}
    assert evidence_for(urls, '2020-11-03', {('url-a', 'legacy-a'): [review]}, {})[0] is None


def test_capture_requires_candidate_key_and_period():
    _, _, urls, _ = fixture()
    review = {'effective_original_url': 'https://example.test/Campaign', 'effective_timestamp': '20201001000000',
              'ai_capture_review_decision': 'support_capture'}
    assert evidence_for(urls, '2020-11-03', {('url-a', 'another-person'): [review]}, {})[0] is None
    review['effective_timestamp'] = '20240101000000'
    assert evidence_for(urls, '2020-11-03', {('url-a', 'legacy-a'): [review]}, {})[0] is None


def test_inventory_capture_can_count_without_successful_replay():
    _, _, urls, _ = fixture()
    validation = {'candidate_identity_match': 'TRUE', 'city_match': 'true', 'mayoral_campaign_match': 'True',
                  'nearest_capture_url': 'https://example.test/Campaign', 'nearest_capture_timestamp': '20201001000000',
                  'status': 'request_error'}
    assert evidence_for(urls, '2020-11-03', {}, {('url-a', 'legacy-a'): [validation]})[0] is True


def test_unknown_archive_flags_not_zero_or_verified():
    _, _, urls, _ = fixture()
    urls[0]['archive_capture_within_headline_window'] = 'true'
    urls[0]['downloaded_content_available'] = 'true'
    assert evidence_for(urls, '2020-11-03', {}, {})[:3] == (None, None, None)


def test_city_year_contradiction_and_duplicate_audit_fail():
    cities, roster, urls, result = fixture()
    override = {'city_id': '0000001', 'year': 2020, 'status': 'no_election', 'source_url': 'https://example.test/calendar'}
    with pytest.raises(ValueError, match='conflicts'):
        build(cities, roster, urls, [result], city_year_overrides=[override])
    with pytest.raises(ValueError, match='Duplicate'):
        build(cities, roster, urls, [], city_year_overrides=[override, override])


def test_malformed_urls_never_establish_capture():
    _, _, urls, _ = fixture()
    urls[0]['exact_normalized_url'] = 'invalid'
    urls[0]['ai_accepted_website'] = 'true'
    urls[0]['ai_review_capture_url'] = ''
    urls[0]['ai_review_capture_timestamp'] = '20201001000000'
    review = {'effective_original_url': '', 'effective_timestamp': '20201001000000', 'ai_capture_review_decision': 'support_capture'}
    assert evidence_for(urls, '2020-11-03', {('url-a', 'legacy-a'): [review]}, {})[0] is None


def test_same_month_distinct_elections_do_not_auto_link_one_legacy_record():
    cities, roster, urls, result = fixture()
    special = deepcopy(result)
    special['election_type'] = 'special'
    special['election_date'] = '2020-11-10'
    with pytest.raises(ValueError, match='multiple elections'):
        build(cities, roster, urls, [result, special])


def test_social_profile_does_not_count_as_campaign_url_lead():
    cities, roster, urls, result = fixture()
    urls[0]['url_type'] = 'social_media'
    output = build(cities, roster, urls, [result])
    assert output[3][0]['url_lead'] is False


def _gap_row(key, **extra):
    row = {
        "candidate_election_id": key, "candidate_name": "A Candidate",
        "election_date": "2013-11-05", "election_id": "0000000__2013-11-05__regular",
    }
    row.update(extra)
    return row


def test_coverage_gaps_merge_is_one_row_per_candidate_election():
    """The checklist asks for one deduplicated gap list, not two tables."""
    missing_urls = [_gap_row("a", queue_group="both_urls_missing"),
                    _gap_row("b", queue_group="one_url_missing")]
    missing_captures = [_gap_row("c", next_action="review_attribution_existing_capture")]
    merged = merge_coverage_gaps(missing_urls, missing_captures)
    assert len(merged) == 3
    assert [r["candidate_election_id"] for r in merged] == ["c", "a", "b"]
    by_key = {r["candidate_election_id"]: r for r in merged}
    assert by_key["a"]["gap_type"] == "no_url_lead"
    assert by_key["a"]["gap_detail"] == "both_urls_missing"
    assert by_key["a"]["next_action"] == "find_campaign_url_from_existing_sources"
    assert by_key["c"]["gap_type"] == "no_period_capture"
    assert by_key["c"]["gap_detail"] == "review_attribution_existing_capture"
    # The existing next_action from the period-capture table is preserved.
    assert by_key["c"]["next_action"] == "review_attribution_existing_capture"


def test_coverage_gaps_merge_refuses_a_repeated_key():
    """A candidate-election must not be able to claim two different gap types."""
    missing_urls = [_gap_row("dup", queue_group="both_urls_missing")]
    missing_captures = [_gap_row("dup", next_action="inspect_inventory_gap_before_new_request")]
    with pytest.raises(ValueError, match="appears in both gap tables"):
        merge_coverage_gaps(missing_urls, missing_captures)


def test_coverage_gaps_merge_does_not_mutate_its_inputs():
    missing_urls = [_gap_row("a", queue_group="both_urls_missing")]
    missing_captures = [_gap_row("c", next_action="review_attribution_existing_capture")]
    merge_coverage_gaps(missing_urls, missing_captures)
    assert "gap_type" not in missing_urls[0]
    assert "gap_type" not in missing_captures[0]


def test_reviewed_identity_link_uses_its_own_basis():
    cities, roster, urls, result = fixture()
    result['candidates'][0]['name'] = 'Exampleton A. Variant'
    roster[0]['full_name'] = 'example variant'
    link = {'election_id': '0000001__2020-11-03__regular', 'candidate_name': 'Exampleton A. Variant',
            'candidate_year_id': 'legacy-a', 'link_basis': 'reviewed_name_variant_same_city_year_month'}
    _, _, _, candidates, _, _ = build(cities, roster, urls, [result], identity_links=[link])
    assert candidates[0]['candidate_year_id'] == 'legacy-a'
    assert candidates[0]['identity_link_basis'] == 'reviewed_name_variant_same_city_year_month'
    assert candidates[0]['url_lead'] is True


def test_reviewed_identity_link_rejects_other_month():
    cities, roster, urls, result = fixture()
    result['candidates'][0]['name'] = 'Exampleton A. Variant'
    roster[0]['month'] = '6'
    link = {'election_id': '0000001__2020-11-03__regular', 'candidate_name': 'Exampleton A. Variant',
            'candidate_year_id': 'legacy-a', 'link_basis': 'reviewed_name_variant_same_city_year_month'}
    with pytest.raises(ValueError, match='Reviewed identity link'):
        build(cities, roster, urls, [result], identity_links=[link])


def _discovered(candidate_id, status='accepted', stamp='20201001120000', url='https://exampleb.test/'):
    return {'candidate_election_id': candidate_id, 'final_status': status, 'campaign_url': url,
            'checked_capture_timestamp': stamp if status == 'accepted' else '',
            'link_basis': 'agent_search_wayback_capture_checked'}


def _runner_up_id(cities, roster, urls, result):
    return next(r for r in build(cities, roster, urls, [result])[3] if r['candidate_name'] == 'Example B')['candidate_election_id']


def test_checked_discovered_url_adds_lead_and_archive_with_own_basis():
    cities, roster, urls, result = fixture()
    key = _runner_up_id(cities, roster, urls, result)
    rows = build(cities, roster, urls, [result], discovered_leads=[_discovered(key)])[3]
    row = next(r for r in rows if r['candidate_election_id'] == key)
    assert row['url_lead'] is True and row['archive_discovery'] is True
    assert row['url_lead_basis'] == 'agent_search_wayback_capture_checked'
    assert row['discovered_campaign_url'] == 'https://exampleb.test/'
    assert 'url_discovery:20201001120000' in row['evidence_refs']
    legacy = next(r for r in rows if r['candidate_name'] == 'Example A')
    assert legacy['url_lead_basis'] == 'legacy_url_inventory'


def test_outside_window_discovery_is_lead_without_archive_and_rejected_is_nothing():
    cities, roster, urls, result = fixture()
    key = _runner_up_id(cities, roster, urls, result)
    row = next(r for r in build(cities, roster, urls, [result], discovered_leads=[_discovered(key, 'outside_window')])[3]
               if r['candidate_election_id'] == key)
    assert row['url_lead'] is True and row['archive_discovery'] is not True
    row = next(r for r in build(cities, roster, urls, [result], discovered_leads=[_discovered(key, 'rejected', url='')])[3]
               if r['candidate_election_id'] == key)
    assert row['url_lead'] is False and row['url_lead_basis'] == ''


def test_discovered_capture_after_election_or_unknown_candidate_fails():
    cities, roster, urls, result = fixture()
    key = _runner_up_id(cities, roster, urls, result)
    with pytest.raises(ValueError, match='before the election'):
        build(cities, roster, urls, [result], discovered_leads=[_discovered(key, stamp='20201201000000')])
    with pytest.raises(ValueError, match='does not match'):
        build(cities, roster, urls, [result], discovered_leads=[_discovered('0000001__2020-11-03__regular__nobody')])


def test_discovered_lead_cannot_replace_legacy_lead():
    cities, roster, urls, result = fixture()
    key = next(r for r in build(cities, roster, urls, [result])[3] if r['candidate_name'] == 'Example A')['candidate_election_id']
    with pytest.raises(ValueError, match='already has a legacy'):
        build(cities, roster, urls, [result], discovered_leads=[_discovered(key)])


def _pages(candidate_id, n='3', verified='True', domain='exampleb.test'):
    return {'candidate_election_id': candidate_id, 'target_id': 'stage3_x', 'registrable_domain': domain,
            'pages_with_text_in_window': n, 'files_verified': verified}


def test_stage3_pages_set_downloaded_content_for_discovered_url():
    cities, roster, urls, result = fixture()
    key = _runner_up_id(cities, roster, urls, result)
    row = next(r for r in build(cities, roster, urls, [result], discovered_leads=[_discovered(key, 'outside_window')],
                                stage3_pages=[_pages(key)])[3] if r['candidate_election_id'] == key)
    assert row['downloaded_content'] is True and row['archive_discovery'] is True
    assert 'stage3_pages:stage3_x:3' in row['evidence_refs']
    row = next(r for r in build(cities, roster, urls, [result], discovered_leads=[_discovered(key)],
                                stage3_pages=[_pages(key, n='0')])[3] if r['candidate_election_id'] == key)
    assert row['downloaded_content'] is not True


def test_stage3_pages_need_verified_hashes_and_matching_domain():
    cities, roster, urls, result = fixture()
    key = _runner_up_id(cities, roster, urls, result)
    with pytest.raises(ValueError, match='hash verification'):
        build(cities, roster, urls, [result], discovered_leads=[_discovered(key)], stage3_pages=[_pages(key, verified='False')])
    with pytest.raises(ValueError, match='discovered campaign domain'):
        build(cities, roster, urls, [result], discovered_leads=[_discovered(key)], stage3_pages=[_pages(key, domain='other.test')])
