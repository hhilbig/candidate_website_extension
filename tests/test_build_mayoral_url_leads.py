"""Checker verdicts change discovery statuses; unrun or missing cities never count as accepted."""
import pytest

from scripts.build_mayoral_url_leads import combine


def _city(*ids):
    return {'slug': 'x', 'finalists': [{'candidate_election_id': i, 'candidate_name': i, 'election_date': '2020-11-03'} for i in ids]}


def _lead(key, status='accepted'):
    return {'candidate_election_id': key, 'status': status, 'campaign_url': 'https://a.test/', 'registrable_domain': 'a.test',
            'checked_capture_url': 'https://web.archive.org/web/20201001120000/https://a.test/', 'lead_sources': [], 'note': ''}


def test_checker_reject_downgrade_and_minor():
    leads = {'x': [_lead('a'), _lead('b'), _lead('c'), _lead('d', 'not_found')]}
    verdicts = {'x': {'checked': 3, 'problems': [
        {'candidate_election_id': 'a', 'severity': 'reject', 'issue': 'other person', 'evidence': ''},
        {'candidate_election_id': 'b', 'severity': 'downgrade', 'issue': 'post-election only', 'evidence': ''},
        {'candidate_election_id': 'c', 'severity': 'minor', 'issue': 'note', 'evidence': ''}]}}
    rows = {r['candidate_election_id']: r for r in combine([_city('a', 'b', 'c', 'd')], leads, verdicts)}
    assert rows['a']['final_status'] == 'rejected' and rows['a']['campaign_url'] == ''
    assert rows['b']['final_status'] == 'outside_window' and rows['b']['checked_capture_timestamp'] == ''
    assert rows['c']['final_status'] == 'accepted' and rows['c']['checked_capture_timestamp'] == '20201001120000'
    assert rows['d']['final_status'] == 'not_found'


def test_unrun_missing_and_unchecked_are_marked():
    assert combine([_city('a')], {}, {})[0]['final_status'] == 'not_run'
    assert combine([_city('a', 'b')], {'x': [_lead('a')]}, {'x': {'problems': []}})[1]['final_status'] == 'missing_from_output'
    assert combine([_city('a')], {'x': [_lead('a')]}, {})[0]['final_status'] == 'accepted_unchecked'


def test_unknown_finalist_or_status_fails():
    with pytest.raises(ValueError):
        combine([_city('a')], {'x': [_lead('z')]}, {})
    with pytest.raises(ValueError):
        combine([_city('a')], {'x': [_lead('a', 'maybe')]}, {})


def test_city_wide_minor_note_is_ignored_but_city_wide_reject_fails():
    note = {'candidate_election_id': 'all', 'severity': 'minor', 'issue': 'no not_found entries', 'evidence': ''}
    rows = combine([_city('a')], {'x': [_lead('a')]}, {'x': {'problems': [note]}})
    assert rows[0]['final_status'] == 'accepted'
    with pytest.raises(ValueError):
        combine([_city('a')], {'x': [_lead('a')]}, {'x': {'problems': [dict(note, severity='reject')]}})


def test_verified_proposal_replaces_not_found_with_own_basis():
    proposal = dict(_lead('a'), page_evidence='X for Mayor', first_capture_in_window='20200101000000')
    rows = combine([_city('a', 'b')], {'x': [_lead('a', 'not_found'), _lead('b', 'not_found')]}, {'x': {'problems': []}},
                   [proposal, dict(_lead('b'), status='not_found', campaign_url='')])
    assert rows[0]['final_status'] == 'accepted' and rows[0]['link_basis'] == 'checker_proposal_wayback_capture_verified'
    assert rows[1]['final_status'] == 'not_found' and rows[1]['link_basis'] == ''


def test_proposal_for_accepted_entry_fails():
    with pytest.raises(ValueError):
        combine([_city('a')], {'x': [_lead('a')]}, {'x': {'problems': []}}, [_lead('a')])


def test_sweep_verdict_applies_after_failed_proposal_and_to_rejected_entries():
    leads = {'x': [_lead('a', 'not_found'), _lead('b')]}
    verdicts = {'x': {'problems': [{'candidate_election_id': 'b', 'severity': 'reject', 'issue': 'other person', 'evidence': ''}]}}
    proposals = [dict(_lead('a'), status='not_found', campaign_url='')]
    sweep = [dict(_lead('a'), page_evidence='A for Mayor'), dict(_lead('b'), page_evidence='B for Mayor')]
    rows = {r['candidate_election_id']: r for r in combine([_city('a', 'b')], leads, verdicts, proposals, sweep)}
    assert rows['a']['link_basis'] == 'domain_sweep_wayback_capture_checked' and rows['a']['final_status'] == 'accepted'
    assert rows['b']['link_basis'] == 'domain_sweep_wayback_capture_checked'


def test_sweep_verdict_for_already_accepted_entry_fails():
    with pytest.raises(ValueError):
        combine([_city('a')], {'x': [_lead('a')]}, {'x': {'problems': []}}, [], [_lead('a')])


def test_repair_verdicts_apply_checker_and_skip_incomplete():
    from scripts.build_mayoral_url_leads import repair_verdicts
    leads = {'x': [_lead('a'), _lead('b'), _lead('c'), _lead('d', 'incomplete'), _lead('e', 'not_found')]}
    verdicts = {'x': {'problems': [{'candidate_election_id': 'b', 'severity': 'reject', 'issue': '', 'evidence': ''},
                                   {'candidate_election_id': 'c', 'severity': 'downgrade', 'issue': '', 'evidence': ''},
                                   {'candidate_election_id': 'a', 'severity': 'minor', 'issue': '', 'evidence': ''}]}}
    items, incomplete = repair_verdicts(leads, verdicts)
    status = {i['candidate_election_id']: i['status'] for i in items}
    assert status == {'a': 'accepted', 'b': 'not_found', 'c': 'outside_window', 'e': 'not_found'} and incomplete == 1
    with pytest.raises(ValueError):
        repair_verdicts(leads, {})


def test_repair_search_replaces_not_found_with_own_basis():
    leads = {'x': [_lead('a', 'not_found'), _lead('b', 'not_found')]}
    repair = [dict(_lead('a'), page_evidence='A for Mayor'), dict(_lead('b'), status='not_found', campaign_url='')]
    rows = {r['candidate_election_id']: r for r in combine([_city('a', 'b')], leads, {'x': {'problems': []}}, repair=repair)}
    assert rows['a']['final_status'] == 'accepted' and rows['a']['link_basis'] == 'agent_search_repair_wayback_capture_checked'
    assert rows['b']['final_status'] == 'not_found' and rows['b']['link_basis'] == ''
