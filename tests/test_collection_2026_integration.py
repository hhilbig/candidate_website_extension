"""Mocked regressions for collection, resume, and pilot forecast integration."""
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import patch

from src.collection_2026 import Store, crawl_snapshot, page_links
from scripts.collect_2026 import forecast, live_schedule_state


CANDIDATE = {"candidate_id": "TEST1", "candidate": "Example Person", "website_url": "https://example.test/",
             "office": "house", "party": "D", "state": "CA", "district": "01", "year": 2026}
EVENT = {"event_id": "test-primary", "date": "2026-06-15", "timezones": ["America/Los_Angeles"]}


def _row(snapshot, url, page_type, depth, source, status):
    return {"page_id": snapshot + '-' + url, "snapshot_id": snapshot, "candidate_id": "TEST1",
            "original_url": url, "effective_url": url, "page_type": page_type, "depth": depth,
            "source": source, "status": status, "identity_status": "confirmed", "snapshot_complete": False,
            "captured_at": "2026-06-14T19:00:00+00:00", "html_path": "example.html.gz", "compressed_bytes": 100}


def test_archive_raw_root_relative_links_use_original_site_origin():
    result = page_links(b'<a href="/issues">Issues</a><a href="about">About</a>',
                        'https://web.archive.org/web/20260614120000id_/https://example.test/', 'wayback', 2)
    assert ('https://example.test/issues', 'issues') in result
    assert ('https://example.test/about', 'biography') in result


def test_malformed_link_is_skipped_instead_of_stopping_the_run():
    """An unbalanced IPv6 bracket raised ValueError and stopped every run from 2026-09-24."""
    result = page_links(b'<a href="http://[broken/issues">Bad</a><a href="/issues">Issues</a>',
                        'https://example.test/', 'live', 2)
    assert result == [('https://example.test/issues', 'issues')]


def test_retry_revisits_failed_subpage_instead_of_accepting_completed_homepage(tmp_path):
    store = Store(tmp_path)
    client = SimpleNamespace(config={"max_pages_per_snapshot": 10, "max_depth": 2})
    issues_calls = []

    def acquire(store, client, candidate, snapshot, url, page_type, depth, source, *args, **kwargs):
        if depth:
            issues_calls.append(url)
        status = 'request_error' if depth and len(issues_calls) == 1 else 'usable'
        row = _row(snapshot, url, page_type, depth, source, status)
        store.page(row)
        return row, b'<a href="https://example.test/issues">Issues</a>' if not depth else b'<html>Issues content</html>'

    with patch('src.collection_2026.acquire_page', side_effect=acquire):
        initial = crawl_snapshot(store, client, CANDIDATE, CANDIDATE['website_url'], 'work', 'live')
        assert initial['retryable']
        resumed = crawl_snapshot(store, client, CANDIDATE, CANDIDATE['website_url'], 'work', 'live')
    assert len(issues_calls) == 2
    assert resumed['usable']
    assert not resumed['retryable']


def test_failed_archived_child_tries_next_eligible_capture(tmp_path):
    store = Store(tmp_path)
    for timestamp in ('20260614180000', '20260613180000'):
        store.capture('inventory', {'timestamp': timestamp, 'original': 'https://example.test/issues',
                                   'statuscode': '200', 'mimetype': 'text/html', 'candidate_id': 'TEST1'})
    store.db.commit()
    client = SimpleNamespace(config={"max_pages_per_snapshot": 10, "max_depth": 2})
    child_urls = []

    def acquire(store, client, candidate, snapshot, url, page_type, depth, source, *args, **kwargs):
        if depth:
            child_urls.append(url)
        status = 'request_error' if depth and '20260614180000' in url else 'usable'
        row = _row(snapshot, url, page_type, depth, source, status)
        store.page(row)
        return row, b'<a href="https://example.test/issues">Issues</a>' if not depth else b'<html>Issues content</html>'

    with patch('src.collection_2026.acquire_page', side_effect=acquire):
        crawl_snapshot(store, client, CANDIDATE,
                       'https://web.archive.org/web/20260614190000id_/https://example.test/',
                       'work', 'wayback', EVENT, 'pre', ['inventory'])
    assert len(child_urls) == 2
    assert any('20260613180000' in url for url in child_urls)


def test_forecast_does_not_accept_failed_replays_as_archive_evidence(tmp_path):
    store = Store(tmp_path)
    store.add_work('live', 'live_snapshot', 'TEST1', {})
    store.update('live', 'complete')
    store.add_work('inventory', 'cdx_inventory', 'TEST1', {})
    store.update('inventory', 'complete')
    store.add_work('archive', 'archive_snapshot', 'TEST1', {})
    store.update('archive', 'retryable')
    store.page(_row('live', 'live', 'homepage', 0, 'live', 'usable'))
    store.page(_row('archive', 'archive', 'homepage', 0, 'wayback', 'request_error'))
    report = forecast(tmp_path, [CANDIDATE], [CANDIDATE], store, {'minimum_free_bytes': 0})
    assert report['forecast_status'] == 'insufficient_pilot_evidence'
    assert not report['storage_fits']


def _forecast_store(tmp_path, monkeypatch, events=()):
    import gzip
    import scripts.collect_2026 as command

    class FrozenDatetime(datetime):
        @classmethod
        def now(cls, tz=None):
            return cls(2026, 11, 10, 12, tzinfo=timezone.utc)

    monkeypatch.setattr(command, 'datetime', FrozenDatetime)
    store = Store(tmp_path)
    store.add_work('live', 'live_snapshot', 'TEST1', {})
    store.update('live', 'complete')
    store.add_work('inventory', 'cdx_inventory', 'TEST1', {})
    store.update('inventory', 'empty')
    html_path, digest = store.blob(b'<html>Example Person ' + b'campaign text ' * 100 + b'</html>')
    text_path = tmp_path / 'text' / 'fixture.txt.gz'
    text_path.parent.mkdir(parents=True)
    text_path.write_bytes(gzip.compress(b'Example Person ' + b'campaign text ' * 100))
    row = _row('live', 'https://example.test/', 'homepage', 0, 'live', 'usable')
    row.update(html_path=html_path, text_path=str(text_path.relative_to(tmp_path)), sha256=digest,
               snapshot_complete=True, captured_at='2026-11-10T12:00:00+00:00')
    store.page(row)
    aid = store.attempt('live', 'https://example.test/')
    store.finish_attempt(aid, 'ok', {'body_path': html_path})
    cdx_path, _ = store.blob(b'[]')
    aid = store.attempt('inventory', 'https://web.archive.org/cdx/search/cdx')
    store.finish_attempt(aid, 'ok', {'body_path': cdx_path})
    config = {'minimum_free_bytes': 0, '_calendar_events': list(events)}
    return store, config, row


def test_live_schedule_reads_latest_complete_homepage_without_loading_corpus(tmp_path, monkeypatch):
    store,config,row=_forecast_store(tmp_path,monkeypatch)
    original=row['captured_at']
    store.page(dict(row,page_id='incomplete',captured_at='2026-11-11T12:00:00+00:00',snapshot_complete=False))
    store.page(dict(row,page_id='archived',captured_at='2026-11-12T12:00:00+00:00',source='wayback'))
    store.page(dict(row,page_id='child',captured_at='2026-11-13T12:00:00+00:00',depth=1,page_type='issues'))
    store.rows=lambda *args: (_ for _ in ()).throw(AssertionError('full table materialized'))
    pages,activity=live_schedule_state(store)
    assert pages['TEST1'][0]['captured_at']==original
    assert len(pages['TEST1'])==1
    assert 'TEST1' in activity


def test_empty_completed_archive_can_forecast_and_counts_html_text_metadata(tmp_path, monkeypatch):
    store, config, row = _forecast_store(tmp_path, monkeypatch)
    roster = tmp_path / 'rosters'
    (roster / 'fec').mkdir(parents=True)
    (roster / 'versions').mkdir()
    (roster / 'fec' / 'example.zip').write_bytes(b'a' * 100)
    (roster / 'current.csv').write_bytes(b'b' * 50)
    (roster / 'versions' / 'example.json').write_bytes(b'c' * 10)
    report = forecast(tmp_path, [CANDIDATE], [CANDIDATE], store, config)
    assert report['forecast_status'] == 'estimated'
    assert report['storage_fits']
    assert report['archive_size_basis'] == 'observed_live_snapshot_proxy'
    assert report['pilot_live_compressed_bytes'] == (
        (tmp_path / row['html_path']).stat().st_size + (tmp_path / row['text_path']).stat().st_size)
    assert report['measured_cdx_body_bytes'] > 0
    assert report['projected_ledger_export_bytes'] > 0
    assert report['measured_roster_refresh_bytes'] == 160
    assert report['projected_roster_version_bytes'] == 640
    assert report['projected_total_bytes'] == sum(report[key] for key in (
        'projected_live_bytes', 'projected_archive_bytes', 'projected_inventory_body_bytes',
        'projected_ledger_export_bytes', 'projected_roster_version_bytes'))


def test_forecast_adds_no_live_copies_after_a_post_general_copy(tmp_path, monkeypatch):
    store, config, _ = _forecast_store(tmp_path, monkeypatch)
    weekly = forecast(tmp_path, [CANDIDATE], [CANDIDATE], store, config)
    event = dict(EVENT, event_id='fixture-general', date='2026-11-03', state='CA', office='house',
                 districts=['*'], parties=['D', 'R'], event_type='general', timezones=['UTC'])
    config['_calendar_events'] = [event]
    daily = forecast(tmp_path, [CANDIDATE], [CANDIDATE], store, config)
    assert weekly['planned_live_snapshot_count'] == 0
    assert daily['planned_live_snapshot_count'] == 0
    assert daily['planned_archive_event_side_count'] == 2
    assert daily['planned_inventory_candidate_rounds'] == 2


def test_forecast_requires_each_nonempty_inventory_event_side_to_finish(tmp_path, monkeypatch):
    event = dict(EVENT, event_id='fixture-general', date='2026-11-03', state='CA', office='house',
                 districts=['*'], parties=['D', 'R'], event_type='general', timezones=['UTC'])
    store, config, _ = _forecast_store(tmp_path, monkeypatch, [event])
    store.update('inventory', 'complete', {'capture_rows': 2})
    report = forecast(tmp_path, [CANDIDATE], [CANDIDATE], store, config)
    assert report['missing_replay_work_count'] == 2
    assert not report['archive_replay_work_complete']
    assert not report['storage_fits']


def test_forecast_accepts_resolved_supersession_and_interrupted_attempts(tmp_path, monkeypatch):
    store, config, _ = _forecast_store(tmp_path, monkeypatch)
    store.add_work('old-inventory', 'cdx_inventory', 'TEST1', {})
    store.update('old-inventory', 'superseded', {'superseded_by': 'inventory'})
    aid = store.attempt('live', 'https://example.test/')
    store.finish_attempt(aid, 'interrupted', {})
    report = forecast(tmp_path, [CANDIDATE], [CANDIDATE], store, config)
    assert report['forecast_status'] == 'estimated'
    store.update('old-inventory', 'superseded', {'superseded_by': 'missing-work'})
    report = forecast(tmp_path, [CANDIDATE], [CANDIDATE], store, config)
    assert report['forecast_status'] == 'insufficient_pilot_evidence'
    assert report['unresolved_work_count'] == 1


def test_large_capture_inventory_does_not_inflate_live_page_metadata(tmp_path, monkeypatch):
    from src.collection_2026_outputs import _stream_export

    store, config, _ = _forecast_store(tmp_path, monkeypatch)
    before = forecast(tmp_path, [CANDIDATE], [CANDIDATE], store, config)
    for index in range(10000):
        store.capture('inventory', {'timestamp': '20260614180000',
                                   'original': f'https://example.test/news/{index}',
                                   'statuscode': '200', 'mimetype': 'text/html', 'candidate_id': 'TEST1',
                                   'digest': 'EXAMPLE-DIGEST', 'length': '20000'})
    store.db.commit()
    (tmp_path / 'exports').mkdir(exist_ok=True)
    _stream_export(tmp_path / 'exports', 'capture_inventory', lambda: iter(store.rows('captures')), parquet=False, csv_gzip=True)
    after = forecast(tmp_path, [CANDIDATE], [CANDIDATE], store, config)
    assert after['database_allocation_basis'] == 'dbstat_tables_and_indexes'
    for label in ('pages_live', 'attempts_live', 'work_live'):
        assert after['metadata_unit_bytes'][label] == before['metadata_unit_bytes'][label]
        assert after['projected_metadata_bytes']['sqlite_' + label] == before['projected_metadata_bytes']['sqlite_' + label]
    assert after['projected_metadata_rows']['captures'] == 10000
    assert after['projected_metadata_bytes']['sqlite_captures'] > 0
    assert after['projected_metadata_bytes']['export_capture_inventory.csv.gz'] > 0
    assert after['projected_ledger_export_bytes'] == sum(after['projected_metadata_bytes'].values())


def test_forecast_without_page_sample_reports_missing_evidence(tmp_path, monkeypatch):
    store, config, _ = _forecast_store(tmp_path, monkeypatch)
    store.db.execute('DELETE FROM pages')
    store.db.commit()
    report = forecast(tmp_path, [CANDIDATE], [CANDIDATE], store, config)
    assert report['forecast_status'] == 'insufficient_pilot_evidence'
    assert 'no_usable_live_snapshot_size_sample' in report['insufficient_evidence_reasons']


def test_metadata_fallback_uses_record_bytes_when_dbstat_is_unavailable(tmp_path, monkeypatch):
    import sqlite3

    store, config, _ = _forecast_store(tmp_path, monkeypatch)
    database = store.db

    class WithoutDbstat:
        def execute(self, sql, *args):
            if 'FROM dbstat' in sql:
                raise sqlite3.OperationalError('no such table: dbstat')
            return database.execute(sql, *args)

    store.db = WithoutDbstat()
    report = forecast(tmp_path, [CANDIDATE], [CANDIDATE], store, config)
    assert report['database_allocation_basis'] == 'serialized_rows_with_physical_overhead'
    assert report['metadata_unit_bytes']['pages_live'] > 0
    assert report['projected_ledger_export_bytes'] > 0


def _failed_work(store, key, kind, candidate_id='TEST2', count=3, error='ReadTimeout', event_id='', side=''):
    store.add_work(key, kind, candidate_id, {}, event_id, side)
    for _ in range(count):
        aid = store.attempt(key, 'https://example.test/' + key)
        store.finish_attempt(aid, 'request_error', {'error': error})
    result = {'reason': 'request_error'} if kind == 'cdx_inventory' else {'snapshot_id': key, 'retryable': True}
    store.update(key, 'retryable', result)


def test_exhausted_network_failures_are_measured_without_becoming_successes(tmp_path, monkeypatch):
    store, config, _ = _forecast_store(tmp_path, monkeypatch)
    second = dict(CANDIDATE, candidate_id='TEST2')
    _failed_work(store, 'live-failure', 'live_snapshot')
    _failed_work(store, 'inventory-failure', 'cdx_inventory')
    report = forecast(tmp_path, [CANDIDATE, second], [CANDIDATE, second], store, config)
    assert report['forecast_status'] == 'estimated'
    assert report['measured_failure_work_count'] == 2
    assert report['live_pilot_outcomes_measured']
    assert report['archive_inventory_outcomes_measured']
    assert not report['live_work_complete']
    assert not report['archive_inventory_complete']
    assert {row['status'] for row in store.rows('work') if row['candidate_id'] == 'TEST2'} == {'retryable'}


def test_unexhausted_network_failure_does_not_finish_pilot(tmp_path, monkeypatch):
    store, config, _ = _forecast_store(tmp_path, monkeypatch)
    _failed_work(store, 'unfinished-network', 'live_snapshot', candidate_id='TEST1', count=2)
    report = forecast(tmp_path, [CANDIDATE], [CANDIDATE], store, config)
    assert report['forecast_status'] == 'insufficient_pilot_evidence'
    assert report['unmeasured_work_reasons']['unfinished-network'] == 'request_retry_budget_not_exhausted'


def test_parser_and_local_implementation_errors_are_not_measured_network_failures(tmp_path, monkeypatch):
    store, config, _ = _forecast_store(tmp_path, monkeypatch)
    store.add_work('parser', 'cdx_inventory', 'TEST1', {})
    aid = store.attempt('parser', 'https://web.archive.org/cdx/search/cdx')
    store.finish_attempt(aid, 'ok', {'http_status': 200})
    store.update('parser', 'retryable', {'reason': 'CDX header incomplete'})
    _failed_work(store, 'disk-error', 'live_snapshot', candidate_id='TEST1', error='PermissionError')
    report = forecast(tmp_path, [CANDIDATE], [CANDIDATE], store, config)
    assert report['forecast_status'] == 'insufficient_pilot_evidence'
    assert report['unmeasured_work_reasons']['parser'] == 'parser_or_implementation_error'
    assert report['unmeasured_work_reasons']['disk-error'] == 'unclassified_or_implementation_error'


def test_pending_started_and_interrupted_work_remain_unmeasured(tmp_path, monkeypatch):
    store, config, _ = _forecast_store(tmp_path, monkeypatch)
    store.add_work('pending', 'live_snapshot', 'TEST1', {})
    store.add_work('interrupted', 'live_snapshot', 'TEST1', {})
    aid = store.attempt('interrupted', 'https://example.test/interrupted')
    store.finish_attempt(aid, 'interrupted', {})
    store.update('interrupted', 'retryable', {'snapshot_id': 'interrupted'})
    store.add_work('running', 'live_snapshot', 'TEST1', {})
    store.start('running')
    store.attempt('running', 'https://example.test/running')
    report = forecast(tmp_path, [CANDIDATE], [CANDIDATE], store, config)
    assert report['forecast_status'] == 'insufficient_pilot_evidence'
    assert set(report['unmeasured_work_reasons']) == {'pending', 'interrupted', 'running'}
    assert report['unfinished_attempt_count'] == 1


def test_observed_content_challenge_is_a_measured_failure(tmp_path, monkeypatch):
    store, config, row = _forecast_store(tmp_path, monkeypatch)
    second = dict(CANDIDATE, candidate_id='TEST2')
    store.add_work('challenge', 'live_snapshot', 'TEST2', {})
    aid = store.attempt('challenge', 'https://example.test/challenge')
    store.finish_attempt(aid, 'ok', {'http_status': 200, 'body_path': row['html_path']})
    failed_page = dict(row, page_id='challenge', candidate_id='TEST2', snapshot_id='challenge',
                       status='request_error', error='service_or_browser_challenge', snapshot_complete=False)
    store.page(failed_page)
    store.update('challenge', 'retryable', {'snapshot_id': 'challenge', 'retryable': True})
    store.add_work('second-empty', 'cdx_inventory', 'TEST2', {})
    store.update('second-empty', 'empty')
    report = forecast(tmp_path, [CANDIDATE, second], [CANDIDATE, second], store, config)
    assert report['forecast_status'] == 'estimated'
    assert report['measured_failure_work_keys'] == ['challenge']


def test_archive_replay_failure_can_complete_measurement_after_retry_budget(tmp_path, monkeypatch):
    event = dict(EVENT, event_id='fixture-general', date='2026-11-03', state='CA', office='house',
                 districts=['*'], parties=['D', 'R'], event_type='general', timezones=['UTC'])
    store, config, _ = _forecast_store(tmp_path, monkeypatch, [event])
    store.update('inventory', 'complete', {'capture_rows': 2})
    for side in ('pre', 'post'):
        _failed_work(store, 'archive-' + side, 'archive_snapshot', candidate_id='TEST1', event_id=event['event_id'], side=side)
    report = forecast(tmp_path, [CANDIDATE], [CANDIDATE], store, config)
    assert report['forecast_status'] == 'estimated'
    assert report['archive_replay_outcomes_measured']
    assert not report['archive_replay_work_complete']
    assert report['archive_size_basis'] == 'observed_live_snapshot_proxy'


def test_failed_inventory_keeps_unavailable_replays_explicit(tmp_path, monkeypatch):
    event = dict(EVENT, event_id='fixture-general', date='2026-11-03', state='CA', office='house',
                 districts=['*'], parties=['D', 'R'], event_type='general', timezones=['UTC'])
    store, config, _ = _forecast_store(tmp_path, monkeypatch, [event])
    store.update('inventory', 'complete', {'capture_rows': 2})
    _failed_work(store, 'www-inventory-failure', 'cdx_inventory', candidate_id='TEST1')
    report = forecast(tmp_path, [CANDIDATE], [CANDIDATE], store, config)
    assert report['forecast_status'] == 'estimated'
    assert report['inventory_failure_candidate_count'] == 1
    assert report['missing_replay_work_count'] == 0
    assert not report['archive_inventory_complete']


def test_forecast_queries_only_pilot_records_without_materializing_full_store(tmp_path, monkeypatch):
    store, config, row = _forecast_store(tmp_path, monkeypatch)
    store.page(dict(row, candidate_id='OUTSIDE', page_id='outside', snapshot_id='outside', html_path='not-present'))
    store.add_work('outside', 'live_snapshot', 'OUTSIDE', {})
    store.attempt('outside', 'https://outside.test/')
    monkeypatch.setattr(store, 'rows', lambda table: (_ for _ in ()).throw(AssertionError('unfiltered Store.rows call')))
    report = forecast(tmp_path, [CANDIDATE], [CANDIDATE], store, config)
    assert report['forecast_status'] == 'estimated'
    assert report['pilot_requests'] == 2
    assert report['missing_artifact_count'] == 0
    assert report['unfinished_attempt_count'] == 0


def test_base_estimate_is_distinct_from_two_times_planning_allowance(tmp_path, monkeypatch):
    store, config, _ = _forecast_store(tmp_path, monkeypatch)
    report = forecast(tmp_path, [CANDIDATE], [CANDIDATE], store, config)
    assert report['base_estimated_total_bytes'] == sum(report['base_estimate_components'].values())
    assert report['planning_allowance_total_bytes'] == 2 * report['base_estimated_total_bytes']
    assert report['projected_total_bytes'] == report['planning_allowance_total_bytes']
    assert report['storage_gate_basis'] == 'planning_allowance_total_bytes_plus_headroom'


def test_capture_estimate_counts_each_history_once_and_ignores_stale_csv(tmp_path, monkeypatch):
    from src.collection_2026_outputs import _stream_export
    store, config, _ = _forecast_store(tmp_path, monkeypatch)
    row = {'candidate_id': 'TEST1', 'timestamp': '20260614180000', 'original': 'https://example.test/about',
           'mimetype': 'text/html', 'statuscode': '200', 'digest': 'TEST', 'length': '1000'}
    store.capture('inventory', row)
    store.add_work('later-inventory', 'cdx_inventory', 'TEST1', {'through_date': '20261110'})
    store.update('later-inventory', 'complete')
    store.capture('later-inventory', row)
    store.db.commit()
    exports = tmp_path / 'exports'
    exports.mkdir()
    _stream_export(exports, 'capture_inventory', lambda: iter(store.rows('captures')), parquet=False, csv_gzip=True)
    legacy = exports / 'capture_inventory.csv'
    legacy.write_bytes(b'legacy asset metadata' * 10000)
    report = forecast(tmp_path, [CANDIDATE], [CANDIDATE, dict(CANDIDATE, candidate_id='TEST2')], store, config)
    assert report['pilot_unique_eligible_capture_count'] == 1
    assert report['projected_metadata_rows']['captures'] == 2
    assert str(legacy) in report['ignored_legacy_inventory_exports']
    assert 'export_capture_inventory.csv' not in report['projected_metadata_bytes']
    assert report['projected_metadata_bytes']['export_capture_inventory.csv.gz'] == 4 * (exports / 'capture_inventory.csv.gz').stat().st_size
    assert report['inventory_export_size_basis'] == 'observed_gzip_export'


def test_future_cdx_estimate_preserves_broad_response_storage(tmp_path, monkeypatch):
    from src.collection_2026 import CDX_ASSET_MIME_FILTER
    store, config, _ = _forecast_store(tmp_path, monkeypatch)
    body_path, _ = store.blob(bytes(range(256)) * 500)
    aid = store.attempt('inventory', 'https://web.archive.org/cdx/search/cdx?old_unfiltered')
    store.finish_attempt(aid, 'ok', {'body_path': body_path})
    store.capture('inventory', {'candidate_id': 'TEST1', 'timestamp': '20260614180000',
                               'original': 'https://example.test/about', 'mimetype': 'text/html', 'statuscode': '200'})
    store.capture('inventory', {'candidate_id': 'TEST1', 'timestamp': '20260614180000',
                               'original': 'https://example.test/large.jpg', 'mimetype': 'image/jpeg', 'statuscode': '200'})
    store.db.commit()
    report = forecast(tmp_path, [CANDIDATE], [CANDIDATE], store, config)
    assert report['pilot_unique_eligible_capture_count'] == 1
    assert 'filtered_cdx_proxy_compressed_bytes' not in report
    assert report['future_cdx_body_basis'] == 'measured_broad_cdx_gzip_bytes_per_candidate_inventory_round'
    assert report['future_cdx_server_filter'] is CDX_ASSET_MIME_FILTER is None
    assert report['base_estimate_components']['projected_inventory_body_bytes'] == (
        report['measured_cdx_body_bytes'] * report['planned_inventory_candidate_rounds'])


def test_after_end_date_only_the_archive_phase_runs(tmp_path, monkeypatch, capsys):
    """Hanno approved on 2026-10-24: one Wayback archive pass in December, no live copies after the end date."""
    import json
    import sys
    import scripts.collect_2026 as command

    class December(datetime):
        @classmethod
        def now(cls, tz=None):
            return cls(2026, 12, 5, 16, tzinfo=timezone.utc)

    config = tmp_path / 'config.json'
    config.write_text(json.dumps({'collection_end_date': '2026-11-17'}))
    calls = []
    monkeypatch.setattr(command, 'datetime', December)
    monkeypatch.setattr(command, 'roster', lambda root, pilot=False: [dict(CANDIDATE)])
    monkeypatch.setattr(command, 'Client', lambda store, config: object())
    monkeypatch.setattr(command, 'collect_live', lambda *a, **k: calls.append('live'))
    monkeypatch.setattr(command, 'collect_archive', lambda *a, **k: calls.append('archive'))
    monkeypatch.setattr(command, 'export', lambda *a, **k: {})
    data = tmp_path / 'data'
    for phase in ('all', 'live'):
        monkeypatch.setattr(sys, 'argv', ['collect_2026.py', 'run', '--root', str(data), '--config', str(config),
                                          '--phase', phase])
        command.main()
        assert json.loads(capsys.readouterr().out.strip().splitlines()[-1])['reason'] == 'collection_end_date_passed'
    assert calls == []
    monkeypatch.setattr(sys, 'argv', ['collect_2026.py', 'run', '--root', str(data), '--config', str(config),
                                      '--phase', 'archive'])
    command.main()
    assert calls == ['archive']
    assert not (data / 'rosters' / 'latest_refresh.json').exists()
