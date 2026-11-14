#!/usr/bin/env python3
"""Run the separate 2026 collection and export its current evidence."""
from __future__ import annotations

import argparse
import fcntl
import json
import os
import shutil
import sys
import time
import hashlib
import zipfile
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))

from src.collection_2026 import Store, Client, collect_live, collect_archive, stable, write_json, utcnow


def roster(root, pilot=False):
    from src.collection_2026_roster import load_roster
    if pilot:
        path = root / 'pilot_roster.json'
        return json.loads(path.read_text())
    return load_roster(root)


def live_schedule_state(store):
    """Keep one usable live date per candidate, without loading the page corpus."""
    latest={}
    for cid,stamp in store.db.execute("""
        SELECT json_extract(data,'$.candidate_id'),MAX(json_extract(data,'$.captured_at'))
        FROM pages WHERE json_extract(data,'$.source')='live'
          AND json_extract(data,'$.page_type')='homepage' AND json_extract(data,'$.depth')=0
          AND json_extract(data,'$.status')='usable' AND json_extract(data,'$.identity_status')='confirmed'
          AND json_extract(data,'$.snapshot_complete')=1
        GROUP BY json_extract(data,'$.candidate_id')
    """):
        latest[cid]=[dict(candidate_id=cid,captured_at=stamp,source='live',page_type='homepage',
                          depth=0,status='usable',identity_status='confirmed',snapshot_complete=True)]
    activity={row[0]:row[1] for row in store.db.execute(
        "SELECT candidate_id,MAX(updated_at) FROM work WHERE kind='live_snapshot' GROUP BY candidate_id")}
    return latest,activity


def export(root, candidates, events, store):
    from src.collection_2026_outputs import write_store_outputs
    report = write_store_outputs(root / 'exports', candidates, events, store)
    report['disk_free_bytes'] = shutil.disk_usage(root).free
    report['written_at'] = utcnow()
    write_json(root / 'exports' / 'coverage_report.json', report)
    return report


def forecast(root, candidates, all_candidates, store, config):
    """Estimate storage and requests from completed pilot work and calendar cadence."""
    from datetime import date, timedelta
    from src.collection_2026_calendar import load_events, events_for_candidate
    from src.collection_2026_outputs import _datetime, _local_dates, _regular_general_date, NONPARTICIPANT

    root = Path(root)
    now = datetime.now(timezone.utc)
    events = config.get('_calendar_events')
    if events is None:
        events = load_events(config.get('calendar_path', REPO / 'config/collection_2026_calendar.json'))
    pilot_ids = {c['candidate_id'] for c in candidates}
    placeholders = ','.join('?' for _ in pilot_ids)
    pilot_parameters = tuple(sorted(pilot_ids))
    # Keep Python memory proportional to the pilot when the full corpus grows.
    pages = [json.loads(row['data']) for row in store.db.execute(
        f"SELECT data FROM pages WHERE json_extract(data,'$.candidate_id') IN ({placeholders})", pilot_parameters)] if pilot_ids else []
    work = [dict(row) for row in store.db.execute(
        f'SELECT * FROM work WHERE candidate_id IN ({placeholders})', pilot_parameters)] if pilot_ids else []
    for row in work:
        row['payload'] = json.loads(row['payload'])
        row['result'] = json.loads(row['result'])
    attempts = [dict(row) for row in store.db.execute(
        f'SELECT a.* FROM attempts a JOIN work w ON a.work_key=w.key WHERE w.candidate_id IN ({placeholders})',
        pilot_parameters)] if pilot_ids else []
    for row in attempts:
        row['detail'] = json.loads(row['detail'])
    inventories = [w for w in work if w['kind'] == 'cdx_inventory']
    live_work = [w for w in work if w['kind'] == 'live_snapshot']
    replay_work = [w for w in work if w['kind'] == 'archive_snapshot']

    work_by_key = {w['key']: w for w in work}
    attempts_by_work = {}
    for attempt in attempts:
        attempts_by_work.setdefault(attempt['work_key'], []).append(attempt)
    retry_budget = max(1, int(config.get('request_attempts', 3)))
    network_errors = {'ConnectionError', 'ConnectTimeout', 'ReadTimeout', 'Timeout', 'SSLError',
                      'ChunkedEncodingError', 'ContentDecodingError', 'ProxyError', 'RetryError'}
    retry_http = {408, 425, 429, 500, 502, 503, 504}
    bounded_errors = {'unsafe_or_external_redirect', 'redirect_loop_or_limit'}

    def request_failure(attempt):
        detail = attempt.get('detail', {})
        return (attempt.get('status') == 'request_error' and
                (detail.get('error') in network_errors or detail.get('http_status') in retry_http))

    def assessed(item):
        """An attempted failure is evidence; it is not operational completion."""
        seen = set()
        while item.get('status') == 'superseded':
            if item.get('key') in seen:
                return 'unmeasured', 'invalid_supersession'
            seen.add(item.get('key'))
            item = work_by_key.get(item.get('result', {}).get('superseded_by'), {})
        if seen:
            return ('superseded', '') if item.get('status') in {'complete', 'empty'} else ('unmeasured', 'replacement_incomplete')
        status = item.get('status')
        if status in {'pending', 'running'}:
            return 'unmeasured', 'work_not_finished'
        if status == 'blocked' and item.get('result', {}).get('reason') == 'missing_url':
            return 'missing_url', ''
        history = sorted(attempts_by_work.get(item.get('key'), []), key=lambda a: (a['started_at'], a['attempt_id']))
        groups = {}
        for attempt in history:
            groups.setdefault(attempt['request_url'], []).append(attempt)
        for series in groups.values():
            last = series[-1]
            error = last.get('detail', {}).get('error', '')
            if last.get('status') == 'started':
                return 'unmeasured', 'attempt_started'
            if last.get('status') == 'request_error' and not request_failure(last) and error != 'render_TimeoutError':
                return 'unmeasured', 'unclassified_or_implementation_error'
        if status in {'complete', 'empty'}:
            return 'completed', ''
        result = item.get('result') or {}
        if status not in {'retryable', 'failed', 'blocked'} or not result or not history:
            return 'unmeasured', 'no_final_attempted_outcome'
        reason = result.get('reason', '')
        if reason and reason not in {'request_error', 'http_error', 'blocked', 'oversize'}:
            return 'unmeasured', 'parser_or_implementation_error'
        if item.get('kind') == 'cdx_inventory' and not reason:
            return 'unmeasured', 'inventory_has_no_final_outcome'
        if item.get('kind') != 'cdx_inventory' and not any(result.get(key) for key in ('snapshot_id', 'snapshots', 'alternative_results')):
            return 'unmeasured', 'crawl_has_no_final_outcome'
        finalized = _datetime(item.get('updated_at'))
        if finalized and any(_datetime(a.get('finished_at')) and _datetime(a['finished_at']) > finalized for a in history):
            return 'unmeasured', 'work_interrupted_after_last_result'
        failure_seen = False
        for series in groups.values():
            last = series[-1]
            detail = last.get('detail', {})
            state = last.get('status')
            if state == 'interrupted' or not last.get('finished_at'):
                return 'unmeasured', 'attempt_interrupted'
            if request_failure(last):
                tail = 0
                for attempt in reversed(series):
                    if not request_failure(attempt):
                        break
                    tail += 1
                if tail < retry_budget:
                    return 'unmeasured', 'request_retry_budget_not_exhausted'
                failure_seen = True
            elif state == 'request_error' and detail.get('error') == 'render_TimeoutError':
                failure_seen = True
            elif state == 'http_error' and int(detail.get('http_status') or 0) >= 400:
                failure_seen = True
            elif state == 'blocked' and detail.get('error') in bounded_errors:
                failure_seen = True
            elif state == 'oversize':
                failure_seen = True
            elif state != 'ok':
                return 'unmeasured', 'unclassified_attempt_outcome'
        # A completed response can still be an observed browser/service challenge.
        snapshot_ids = set()
        def visit(value):
            if isinstance(value, dict):
                if value.get('snapshot_id'):
                    snapshot_ids.add(value['snapshot_id'])
                for child in value.values():
                    visit(child)
            elif isinstance(value, list):
                for child in value:
                    visit(child)
        visit(result)
        content_failure = any(p.get('snapshot_id') in snapshot_ids and p.get('status') == 'request_error'
                              and p.get('error') == 'service_or_browser_challenge' and p.get('html_path') for p in pages)
        if not failure_seen and not content_failure:
            return 'unmeasured', 'no_classified_failure_evidence'
        return 'attempted_failure', ''

    work_assessments = {w['key']: assessed(w) for w in work}
    def measured(item):
        return work_assessments.get(item.get('key'), ('unmeasured', ''))[0] != 'unmeasured'
    initial_complete = all(any(w['candidate_id'] == cid and measured(w) for w in live_work) for cid in pilot_ids)
    inventory_complete = all(any(w['candidate_id'] == cid and measured(w) for w in inventories) for cid in pilot_ids)
    unfinished = [w['key'] for w in work if not measured(w)]
    unfinished_attempts = [a['attempt_id'] for a in attempts if a['status'] == 'started']
    required_replays, inaccessible_inventory_candidates = [], []
    for candidate in candidates:
        rows = [w for w in inventories if w['candidate_id'] == candidate['candidate_id'] and w['status'] != 'superseded']
        # The collector cannot select replays until every required inventory succeeds.
        # Exhausted inventory failures remain explicit missing coverage.
        if rows and any(work_assessments[w['key']][0] == 'attempted_failure' for w in rows):
            inaccessible_inventory_candidates.append(candidate['candidate_id'])
            continue
        if not any(w['status'] == 'complete' for w in rows):
            continue
        for event in events_for_candidate(candidate, events):
            if event.get('participation') in NONPARTICIPANT or date.fromisoformat(event['date']) > now.date() + timedelta(days=30):
                continue
            for side in ('pre', 'post'):
                if not any(w.get('event_id') == event['event_id'] and w.get('side') == side
                           and w['candidate_id'] == candidate['candidate_id'] and measured(w) for w in replay_work):
                    required_replays.append((candidate['candidate_id'], event['event_id'], side))
    replay_complete = not required_replays and all(measured(w) for w in replay_work)

    missing_artifacts = set()
    def file_bytes(relative):
        if not relative:
            return 0
        path = root / relative
        if not path.is_file():
            missing_artifacts.add(str(relative))
            return 0
        return path.stat().st_size

    page_bytes = {p['page_id']: file_bytes(p.get('html_path')) + file_bytes(p.get('text_path')) for p in pages}
    usable_roots = [p for p in pages if p.get('depth') == 0 and p.get('status') == 'usable'
                    and p.get('identity_status') == 'confirmed' and p.get('snapshot_complete')]
    def snapshot_sample(source):
        ids = {p['snapshot_id'] for p in usable_roots if p['source'] == source}
        sizes = [sum(page_bytes[p['page_id']] for p in pages if p['snapshot_id'] == sid) for sid in ids]
        relevant_work = {w['key'] for w in work if w['kind'] == ('live_snapshot' if source == 'live' else 'archive_snapshot')}
        request_count = sum(a['work_key'] in relevant_work for a in attempts)
        requests_per_snapshot = request_count / len(ids) if ids else 0
        return sizes, max(requests_per_snapshot, 1) if ids else 0

    live_sizes, live_requests = snapshot_sample('live')
    archive_sizes, archive_requests = snapshot_sample('wayback')
    mean_live = sum(live_sizes) / len(live_sizes) if live_sizes else 0
    # Empty archive inventories are evidence, but do not measure replay page sizes.
    # Use observed live copies as a planning proxy when the pilot has no replay sample.
    mean_archive = sum(archive_sizes) / len(archive_sizes) if archive_sizes else mean_live
    archive_requests = archive_requests or live_requests
    calendar_matches = {c['candidate_id']: events_for_candidate(c, events) for c in all_candidates}
    planned_live, planned_inventory, planned_archive_sides, planned_archive_jobs = 0, 0, 0, 0
    endpoints, per_candidate_rounds = [], []
    completed_by_candidate = {}
    for page in usable_roots:
        if page['source'] == 'live' and _datetime(page.get('captured_at')):
            cid = page['candidate_id']
            completed_by_candidate[cid] = max(completed_by_candidate.get(cid, datetime.min.replace(tzinfo=timezone.utc)),
                                               _datetime(page['captured_at']))
    for candidate in all_candidates:
        matched = [e for e in calendar_matches[candidate['candidate_id']] if e.get('participation') not in NONPARTICIPANT]
        ending_events = [_regular_general_date(int(float(candidate.get('year') or 2026)))]
        ending_events += [date.fromisoformat(e['date']) for e in matched
                          if e['event_type'] in {'general', 'general_runoff', 'special_general', 'special_runoff'}]
        endpoint = max(ending_events) + timedelta(days=14)
        if config.get('collection_end_date'):
            endpoint = min(endpoint, date.fromisoformat(config['collection_end_date']))
        endpoints.append(endpoint)
        last = completed_by_candidate.get(candidate['candidate_id'])
        count = 0
        for offset in range(max(0, (endpoint - now.date()).days + 2)):
            proposed = now + timedelta(days=offset)
            local = [day for event in matched for day in _local_dates(proposed, event)] or _local_dates(proposed, {})
            if min(local) > endpoint:
                continue
            from src.collection_2026_outputs import live_cadence_due
            if live_cadence_due(matched, last, proposed):
                count += 1
                last = proposed
        per_candidate_rounds.append(count)
        planned_live += count
        candidate_inventory_rounds = max(0, (endpoint - now.date()).days // 7 + 1)
        planned_inventory += candidate_inventory_rounds
        planned_archive_sides += 2 * len(matched)
        planned_archive_jobs += 2 * len(matched) * candidate_inventory_rounds

    from src.collection_2026 import capture_omission_reason, CDX_ASSET_MIME_FILTER
    import csv
    import gzip
    import io
    inventory_keys = {w['key'] for w in inventories}
    cdx_attempts = [a for a in attempts if a['work_key'] in inventory_keys]
    cdx_bytes = sum(file_bytes(a.get('detail', {}).get('body_path')) for a in cdx_attempts)
    inventory_passes = {(w['candidate_id'], w.get('payload', {}).get('through_date', 'initial')) for w in inventories}
    observed_inventory_passes = max(1, len(inventory_passes))
    history_candidates = {w['candidate_id'] for w in inventories}
    observed_histories = max(1, len(history_candidates))
    cdx_requests_per_candidate = len(cdx_attempts) / observed_inventory_passes
    inventory_jobs_per_pass = len(inventories) / observed_inventory_passes

    def eligible_capture_rows(candidate_id=None):
        ids = (candidate_id,) if candidate_id is not None else pilot_parameters
        marks = ','.join('?' for _ in ids)
        if not ids:
            return
        for saved in store.db.execute(f"""
            SELECT data FROM captures WHERE json_extract(data,'$.candidate_id') IN ({marks})
            GROUP BY json_extract(data,'$.candidate_id'),json_extract(data,'$.timestamp'),json_extract(data,'$.original')
            ORDER BY json_extract(data,'$.candidate_id'),json_extract(data,'$.timestamp'),json_extract(data,'$.original')
        """, ids):
            row = json.loads(saved['data'])
            if not capture_omission_reason(row):
                yield row

    # Broad raw CDX responses preserve redirects with misleading MIME metadata.
    # Retained working-table rows still use the local asset filter and global identity.
    capture_count = 0
    inventory_columns = dict.fromkeys(['capture_id', 'inventory_key', 'timestamp', 'original'])
    for candidate_id in sorted(history_candidates):
        for row in eligible_capture_rows(candidate_id):
            capture_count += 1
            inventory_columns.update(dict.fromkeys(row))
    cdx_bytes_per_candidate = cdx_bytes / observed_inventory_passes

    class CompressedSize:
        def __init__(self):
            self.size = 0
        def write(self, value):
            self.size += len(value)
            return len(value)
        def flush(self):
            pass
        def tell(self):
            return self.size

    # This bounded-memory proxy is used only if the new gzip export does not exist yet.
    counter = CompressedSize()
    with gzip.GzipFile(filename='', mode='wb', fileobj=counter, mtime=0) as compressed:
        with io.TextIOWrapper(compressed, encoding='utf-8', newline='') as text_stream:
            writer = csv.DictWriter(text_stream, fieldnames=list(inventory_columns))
            writer.writeheader()
            for row in eligible_capture_rows():
                writer.writerow({key: json.dumps(value, sort_keys=True, ensure_ascii=False)
                                 if isinstance(value, (list, dict, tuple)) else value for key, value in row.items()})
    inventory_export_proxy_bytes = counter.size

    # Allocate SQLite tables and their indexes to the records that create them.
    # Capture metadata must not increase the unit cost of a live-page request.
    tables = ('captures', 'pages', 'attempts', 'work')
    physical_by_table = {table: 0 for table in tables}
    raw_by_table, count_by_table = {}, {}
    for table in tables:
        columns = [row[1] for row in store.db.execute(f'PRAGMA table_info({table})')]
        expression = '+'.join(f'COALESCE(length(CAST("{column}" AS BLOB)),0)' for column in columns)
        count, size = store.db.execute(f'SELECT COUNT(*),COALESCE(SUM({expression}),0) FROM {table}').fetchone()
        count_by_table[table], raw_by_table[table] = count, size
    dbstat_used = False
    fixed_database_bytes = 0
    try:
        owners = {row[0]: row[1] for row in store.db.execute('SELECT name,tbl_name FROM sqlite_master')}
        for name, allocated in store.db.execute('SELECT name,SUM(pgsize) FROM dbstat GROUP BY name'):
            table = owners.get(name)
            if table in physical_by_table and count_by_table[table]:
                physical_by_table[table] += allocated
            else:
                fixed_database_bytes += allocated
        dbstat_used = True
    except Exception as exc:
        import sqlite3
        if not isinstance(exc, sqlite3.DatabaseError):
            raise
        page_size = store.db.execute('PRAGMA page_size').fetchone()[0]
        page_count = store.db.execute('PRAGMA page_count').fetchone()[0]
        fixed_database_bytes = page_size * (1 + sum(not count_by_table[t] for t in tables))
        variable_bytes = max(0, page_size * page_count - fixed_database_bytes)
        for table in tables:
            physical_by_table[table] = variable_bytes * raw_by_table[table] / max(1, sum(raw_by_table.values()))
    table_factor = {table: max(1, physical_by_table[table] / max(1, raw_by_table[table])) for table in tables}

    def raw_record_size(table, row):
        if table == 'pages':
            return len(json.dumps(row).encode()) + len(str(row['page_id']).encode())
        return sum(len((json.dumps(value) if isinstance(value, (dict, list)) else str(value)).encode())
                   for value in row.values() if value is not None)

    source_pages = {source: [p for p in pages if p['source'] == source] for source in ('live', 'wayback')}
    source_snapshots = {source: {p['snapshot_id'] for p in usable_roots if p['source'] == source}
                        for source in ('live', 'wayback')}
    live_page_rate = len(source_pages['live']) / max(1, len(source_snapshots['live']))
    archive_page_rate = (len(source_pages['wayback']) / len(source_snapshots['wayback'])
                         if source_snapshots['wayback'] else live_page_rate)
    future_rows = {
        'pages_live': planned_live * live_page_rate,
        'pages_wayback': planned_archive_sides * archive_page_rate,
        'attempts_live': planned_live * live_requests,
        'attempts_wayback': planned_archive_sides * archive_requests,
        'attempts_inventory': planned_inventory * cdx_requests_per_candidate,
        'work_live': planned_live,
        'work_inventory': planned_inventory * inventory_jobs_per_pass,
        'work_wayback': planned_archive_jobs,
    }
    row_samples = {
        'pages_live': source_pages['live'], 'pages_wayback': source_pages['wayback'],
        'attempts_live': [a for a in attempts if work_by_key[a['work_key']]['kind'] == 'live_snapshot'],
        'attempts_wayback': [a for a in attempts if work_by_key[a['work_key']]['kind'] == 'archive_snapshot'],
        'attempts_inventory': cdx_attempts,
        'work_live': live_work, 'work_wayback': replay_work, 'work_inventory': inventories,
    }
    unit_bytes, raw_unit_bytes = {}, {}
    for label, sample in row_samples.items():
        table = label.split('_')[0]
        fallback = row_samples.get(table + '_live') or [r for label, values in row_samples.items()
                                                                 if label.startswith(table + '_') for r in values]
        selected = sample or fallback
        raw_unit_bytes[label] = sum(raw_record_size(table, row) for row in selected) / max(1, len(selected))
        unit_bytes[label] = raw_unit_bytes[label] * table_factor[table]
    future_rows['captures'] = capture_count / observed_histories * len(all_candidates)
    raw_unit_bytes['captures'] = raw_by_table['captures'] / max(1, count_by_table['captures'])
    unit_bytes['captures'] = raw_unit_bytes['captures'] * table_factor['captures']
    metadata_projection = {'sqlite_' + label: 2 * future_rows[label] * unit_bytes[label] for label in future_rows}

    # A manifest describes one crawl; copies of the same HTML do not reduce its cost.
    manifest_bytes = 0
    for source, planned in (('live', planned_live), ('wayback', planned_archive_sides)):
        observed_ids = {p['snapshot_id'] for p in source_pages[source]}
        files = [root / 'snapshots' / f'{sid}.json' for sid in observed_ids]
        sizes = [path.stat().st_size for path in files if path.is_file()]
        manifest_bytes += sum(sizes)
        unit = sum(sizes) / len(sizes) if sizes else 0
        metadata_projection['snapshot_manifests_' + source] = 2 * unit * planned
    future_counts = {'pages': future_rows['pages_live'] + future_rows['pages_wayback'],
                     'work': sum(future_rows[label] for label in future_rows if label.startswith('work_')),
                     'captures': future_rows['captures'],
                     'attempts': sum(future_rows[label] for label in future_rows if label.startswith('attempts_'))}
    export_tables = {'page_corpus': 'pages', 'work_status': 'work', 'capture_inventory': 'captures', 'attempts': 'attempts'}
    export_bytes = 0
    ignored_legacy_exports = []
    gzip_inventory_seen = False
    for path in (root / 'exports').glob('*'):
        if not path.is_file():
            continue
        if path.name in {'capture_inventory.csv', 'capture_inventory.parquet'}:
            ignored_legacy_exports.append(str(path))
            continue
        size = path.stat().st_size
        export_bytes += size
        export_stem = Path(path.name.removesuffix('.gz')).stem
        if path.name == 'capture_inventory.csv.gz':
            gzip_inventory_seen = True
        table = export_tables.get(export_stem)
        if table:
            # Each existing export is counted once, with its own record denominator.
            # Page/work export costs use source-specific record lengths as weights.
            if table in {'pages', 'work', 'attempts'}:
                future_raw = sum(future_rows[label] * raw_unit_bytes[label] for label in future_rows
                                 if label.startswith(table + '_'))
                projected = size / max(1, raw_by_table[table]) * future_raw
            else:
                projected = size / max(1, count_by_table[table]) * future_counts[table]
        elif export_stem in {'candidate_roster', 'candidate_coverage'}:
            projected = size * len(all_candidates) / max(1, len(pilot_ids))
        elif export_stem == 'candidate_event_pairs':
            observed_pairs = sum(len(events_for_candidate(c, events)) for c in candidates)
            projected = size * sum(len(rows) for rows in calendar_matches.values()) / max(1, observed_pairs)
        else:
            projected = size
        metadata_projection['export_' + path.name] = 2 * projected
    if not gzip_inventory_seen:
        metadata_projection['export_capture_inventory.csv.gz'] = 2 * inventory_export_proxy_bytes * (
            future_rows['captures'] / capture_count if capture_count else 1)
    wal_bytes = sum(path.stat().st_size for path in root.glob('collection.sqlite-*') if path.is_file())
    metadata_projection['sqlite_fixed_and_wal'] = 2 * (fixed_database_bytes + wal_bytes)
    ledger_bytes = sum(path.stat().st_size for path in root.glob('collection.sqlite*') if path.is_file()) + manifest_bytes + export_bytes
    metadata_base_projection = {label: round(value / 2) for label, value in metadata_projection.items()}
    metadata_projection = {label: 2 * value for label, value in metadata_base_projection.items()}
    roster_files = root / 'rosters'
    latest_zip_bytes = max((p.stat().st_size for p in (roster_files / 'fec').glob('*.zip')), default=0)
    current_roster_bytes = (roster_files / 'current.csv').stat().st_size if (roster_files / 'current.csv').is_file() else 0
    roster_round_bytes = latest_zip_bytes + current_roster_bytes + max(
        (p.stat().st_size for p in (roster_files / 'versions').glob('*.json')), default=0)
    refresh_rounds = max(0, (max(endpoints) - now.date()).days // 7 + 1) if endpoints else 0
    projected_requests = (planned_live * live_requests + planned_archive_sides * archive_requests
                          + planned_inventory * cdx_requests_per_candidate)
    base_components = {
        'projected_live_bytes': round(mean_live * planned_live),
        'projected_archive_bytes': round(mean_archive * planned_archive_sides),
        'projected_inventory_body_bytes': round(cdx_bytes_per_candidate * planned_inventory),
        'projected_ledger_export_bytes': sum(metadata_base_projection.values()),
        'projected_roster_version_bytes': round(roster_round_bytes * refresh_rounds),
    }
    components = {name: 2 * value for name, value in base_components.items()}
    base_total = sum(base_components.values())
    projected_total = sum(components.values())
    reasons = []
    if not pilot_ids:
        reasons.append('empty_pilot')
    if not initial_complete:
        reasons.append('initial_live_work_incomplete')
    if not inventory_complete:
        reasons.append('archive_inventory_incomplete')
    if not replay_complete:
        reasons.append('archive_replay_work_incomplete')
    if unfinished or unfinished_attempts:
        reasons.append('unresolved_work_or_attempts')
    if missing_artifacts:
        reasons.append('recorded_artifacts_missing')
    if not live_sizes and planned_live:
        reasons.append('no_usable_live_snapshot_size_sample')
    sufficient = not reasons
    free = shutil.disk_usage(root).free
    durations = [max(0, (datetime.fromisoformat(a['finished_at']) - datetime.fromisoformat(a['started_at'])).total_seconds())
                 for a in attempts if a.get('finished_at')]
    result = dict(
        pilot_candidates=len(pilot_ids), full_roster_candidates=len(all_candidates),
        live_candidates_with_response=len({p['candidate_id'] for p in usable_roots if p['source'] == 'live'}),
        live_work_complete=all(w['status'] in {'complete', 'empty', 'superseded'} or
            (w['status'] == 'blocked' and w.get('result', {}).get('reason') == 'missing_url') for w in live_work) and initial_complete,
        archive_inventory_complete=all(w['status'] in {'complete', 'empty', 'superseded'} or
            (w['status'] == 'blocked' and w.get('result', {}).get('reason') == 'missing_url') for w in inventories) and inventory_complete,
        live_pilot_outcomes_measured=initial_complete, archive_inventory_outcomes_measured=inventory_complete,
        archive_replay_work_complete=replay_complete and all(w['status'] in {'complete', 'empty', 'superseded'} for w in replay_work),
        archive_replay_outcomes_measured=replay_complete, missing_replay_work_count=len(required_replays),
        measured_failure_work_count=sum(kind == 'attempted_failure' for kind, reason in work_assessments.values()),
        measured_failure_work_keys=[key for key, (kind, reason) in work_assessments.items() if kind == 'attempted_failure'],
        unmeasured_work_reasons={key: reason for key, (kind, reason) in work_assessments.items() if kind == 'unmeasured'},
        inventory_failure_candidate_count=len(inaccessible_inventory_candidates),
        unresolved_work_count=len(unfinished), unfinished_attempt_count=len(unfinished_attempts),
        missing_artifact_count=len(missing_artifacts), missing_artifacts=sorted(missing_artifacts),
        pilot_requests=len(attempts), pilot_live_compressed_bytes=sum(live_sizes),
        pilot_archive_compressed_bytes=sum(archive_sizes), mean_live_candidate_bytes=mean_live,
        max_live_candidate_bytes=max(live_sizes, default=0), mean_archive_snapshot_bytes=mean_archive,
        archive_size_basis='observed_archive_snapshots' if archive_sizes else 'observed_live_snapshot_proxy',
        measured_cdx_body_bytes=cdx_bytes, measured_ledger_export_bytes=ledger_bytes,
        future_cdx_body_basis='measured_broad_cdx_gzip_bytes_per_candidate_inventory_round',
        future_cdx_server_filter=CDX_ASSET_MIME_FILTER,
        local_capture_filter='capture_omission_reason',
        pilot_unique_eligible_capture_count=capture_count, observed_inventory_candidate_histories=len(history_candidates),
        capture_projection_basis='unique_candidate_timestamp_original_once_per_candidate_history',
        inventory_export_size_basis='observed_gzip_export' if gzip_inventory_seen else 'gzip_pilot_csv_proxy',
        ignored_legacy_inventory_exports=ignored_legacy_exports,
        database_allocation_basis='dbstat_tables_and_indexes' if dbstat_used else 'serialized_rows_with_physical_overhead',
        measured_database_table_bytes={table: round(value) for table, value in physical_by_table.items()},
        metadata_unit_bytes={label: round(value, 2) for label, value in unit_bytes.items()},
        projected_metadata_rows={label: round(value) for label, value in future_rows.items()},
        projected_metadata_bytes=metadata_projection,
        observed_inventory_candidate_rounds=len(inventory_passes),
        measured_roster_refresh_bytes=roster_round_bytes, planned_live_snapshot_count=planned_live,
        planned_inventory_candidate_rounds=planned_inventory, planned_archive_event_side_count=planned_archive_sides,
        planned_roster_refresh_rounds=refresh_rounds, max_remaining_live_rounds=max(per_candidate_rounds, default=0),
        collection_endpoint=max(endpoints).isoformat() if endpoints else '', storage_safety_factor=2,
        base_estimated_total_bytes=base_total, base_estimate_components=base_components,
        planning_allowance_total_bytes=projected_total, planning_growth_allowance_bytes=projected_total-base_total,
        projected_total_bytes=projected_total, projected_total_bytes_basis='two_times_base_planning_allowance',
        storage_gate_basis='planning_allowance_total_bytes_plus_headroom',
        available_bytes=free, required_headroom_bytes=config['minimum_free_bytes'],
        estimated_initial_full_requests=round(len(attempts) / max(1, len(pilot_ids)) * len(all_candidates)),
        estimated_initial_full_request_seconds=round(sum(durations) / max(1, len(pilot_ids)) * len(all_candidates)),
        estimated_remaining_logical_requests=round(projected_requests),
        forecast_status='estimated' if sufficient else 'insufficient_pilot_evidence',
        insufficient_evidence_reasons=reasons,
        storage_fits=sufficient and projected_total + config['minimum_free_bytes'] < free,
        caveats=['Live copies use monthly (30-day) cadence plus the day before and after each applicable primary or runoff.',
                 'The workload assumes every roster record obtains a website and every event side has an archive.',
                 'Observed HTML and extracted text sizes count each page observation without content deduplication savings.',
                 'The base estimate extrapolates observed sizes; the separate 2x planning allowance is not an expected actual requirement.',
                 'Capture metadata is deduplicated once per candidate history; its growth is not multiplied by weekly refreshes.',
                 'Future weekly CDX response storage uses measured broad-response gzip bytes; no server-side filter savings are assumed.',
                 'Weekly CDX bodies, ledgers, exports, and roster versions are included with a factor of two for growth.',
                 'Page, request, and work metadata use separate record costs. Capture inventory CSV is gzip compressed.',
                 'SQLite tables and indexes use dbstat allocation when available; WAL and database headers are fixed reserves.',
                 'The growth factor is a planning assumption, not a statistical bound.',
                 'Exhausted network retries and observed content failures count as measured outcomes; operational work stays retryable.',
                 'Parser errors, unfinished work, and requests below the configured retry budget do not count as measured outcomes.',
                 'Logical request counts treat a browser render as one operation; browser subrequests are not separately counted.'],
        created_at=utcnow(), **components)
    write_json(root / 'pilot_forecast.json', result)
    return result

def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('command',choices=['refresh','pilot','run','export','status','forecast','enable-full'])
    parser.add_argument('--root',type=Path,default=Path('data/collection_2026'))
    parser.add_argument('--config',type=Path,default=Path('config/collection_2026.json'))
    parser.add_argument('--calendar',type=Path,default=Path('config/collection_2026_calendar.json'))
    parser.add_argument('--phase',choices=['live','archive','all'],default='all')
    parser.add_argument('--pilot',action='store_true')
    parser.add_argument('--lookup-limit',type=int,default=0)
    parser.add_argument('--max-candidates',type=int)
    parser.add_argument('--max-run-seconds',type=int)
    args=parser.parse_args()
    root=args.root.resolve()
    root.mkdir(parents=True,exist_ok=True)
    config=json.loads(args.config.read_text())
    if args.max_run_seconds:
        config['max_run_seconds']=args.max_run_seconds
    from src.collection_2026_calendar import load_events
    from src.collection_2026_roster import refresh_roster,select_pilot,lookup_candidate_urls
    from src.collection_2026_outputs import due_live
    from dotenv import load_dotenv
    if os.environ.get('COLLECTION_2026_ENV_FILE'):
        load_dotenv(os.environ['COLLECTION_2026_ENV_FILE'], override=False)
    events=load_events(args.calendar)
    config['_calendar_events']=events
    with (root / 'run.lock').open('a') as lock:
        try:
            fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        except BlockingIOError:
            print(json.dumps({'status':'already_running'}))
            return
        store=Store(root)
        store.recover()
        if args.command=='refresh':
            rows=refresh_roster(root,lookup_limit=args.lookup_limit)
            print(json.dumps({'candidates':len(rows),'urls':sum(bool(r.get('website_url')) for r in rows)}))
            return
        if args.command=='pilot':
            if (root/'pilot_roster.json').exists():
                raise SystemExit('Pilot already frozen; do not replace its membership.')
            candidates=select_pilot(roster(root))
            if len(candidates)!=40 or set(Counter((c['office'],c['party']) for c in candidates).values())!={10}:
                raise ValueError('Pilot must contain 10 candidates in each office/party group')
            write_json(root/'pilot_roster.json',candidates)
            write_json(root/'pilot_manifest.json',dict(candidate_ids=[c['candidate_id'] for c in candidates],
                       roster_hash=stable(candidates),config_hash=stable(config),events_hash=stable(events),created_at=utcnow()))
            if args.lookup_limit:
                lookup_candidate_urls(root,candidates,limit=args.lookup_limit)
                refreshed={c['candidate_id']:c for c in roster(root)}
                candidates=[dict(c,**refreshed[c['candidate_id']]) for c in candidates]
                write_json(root/'pilot_roster.json',candidates)
            print(json.dumps({'pilot_candidates':len(candidates),'pilot_path':str(root/'pilot_roster.json')}))
            return
        pilot=args.pilot or not (root/'full_enabled.json').exists()
        candidates=roster(root,pilot)
        if args.command=='enable-full':
            evidence=forecast(root,roster(root,True),roster(root),store,config)
            if not evidence['storage_fits']:
                raise SystemExit('Full deployment needs a complete pilot and sufficient storage. See pilot_forecast.json.')
            write_json(root/'full_enabled.json',dict(authorized_plan='2026-09-14',forecast=evidence,enabled_at=utcnow()))
            print(json.dumps({'status':'full_enabled'}))
            return
        if args.command=='forecast':
            print(json.dumps(forecast(root,roster(root,True),roster(root),store,config),indent=2))
            return
        if args.command in {'export','status'}:
            print(json.dumps(export(root,candidates,events,store),indent=2))
            return
        if args.command=='run':
            client=Client(store,config)
            now=datetime.now(timezone.utc)
            from datetime import timedelta
            from datetime import date as _date
            end_date=_date.fromisoformat(config['collection_end_date']) if config.get('collection_end_date') else None
            past_end=end_date is not None and now.date()>end_date
            # Hanno approved on 2026-10-24: after the end date only the Wayback archive phase may run
            # (one final pass in December for the post-general window); live copies stop on the end date.
            if past_end and args.phase!='archive':
                print(json.dumps({'status':'finished','reason':'collection_end_date_passed',
                                  'collection_end_date':end_date.isoformat()}),flush=True)
                return
            refresh_path=root/'rosters/latest_refresh.json'
            latest=json.loads(refresh_path.read_text()) if refresh_path.exists() else {}
            refreshed_at=datetime.fromisoformat(latest.get('fetched_at') or '2000-01-01T00:00:00+00:00')
            if now-refreshed_at>=timedelta(days=7) and not past_end:
                fresh=refresh_roster(root,lookup_limit=0)
                if pilot:
                    current={r['candidate_id']:r for r in fresh}
                    candidates=[dict(c,**current[c['candidate_id']]) for c in candidates]
                    write_json(root/'pilot_roster.json',candidates)
                else:
                    candidates=fresh
            existing_pages,live_activity=live_schedule_state(store)
            # Work that has never run, then the oldest attempted live copy, goes first.
            # This prevents a run deadline from repeatedly excluding the same candidates.
            candidates=sorted(candidates,key=lambda c:(live_activity.get(c['candidate_id'],''),c['candidate_id']))
            if args.max_candidates:
                candidates=candidates[:args.max_candidates]
            manifest_id=now.strftime('%Y%m%dT%H%M%S')+'-'+stable(candidates,args.phase)[:8]
            manifest_dir=root/'run_manifests'
            manifest_dir.mkdir(exist_ok=True)
            code_paths=sorted((REPO/'src').glob('collection_2026*.py'))+[Path(__file__).resolve(),
                        REPO/'src/extract_text.py',REPO/'src/name_utils.py',REPO/'src/build_candidate_roster.py']
            with zipfile.ZipFile(manifest_dir/f'{manifest_id}_code.zip','w',zipfile.ZIP_DEFLATED) as bundle:
                for path in code_paths:
                    bundle.write(path,str(path.relative_to(REPO)))
            write_json(manifest_dir/f'{manifest_id}.json',dict(created_at=utcnow(),phase=args.phase,pilot=pilot,
                       candidates=candidates,events=events,config={k:v for k,v in config.items() if not k.startswith('_')},
                       code_sha256={str(path.relative_to(REPO)):hashlib.sha256(path.read_bytes()).hexdigest() for path in code_paths},
                       candidate_hash=stable(candidates),event_hash=stable(events)))
            stop_reason=''
            write_json(root/'run_status.json',dict(status='running',pid=os.getpid(),phase=args.phase,pilot=pilot,started_at=utcnow()))
            try:
                # Complete the urgent live pass before any potentially slow archive work.
                if args.phase in {'live','all'}:
                    for candidate in candidates:
                        if not candidate.get('website_url') or due_live(candidate,events,existing_pages.get(candidate['candidate_id'],[]),now,end_date):
                            collect_live(store,client,candidate,now.date().isoformat())
                            print(json.dumps({'phase':'live','candidate_id':candidate['candidate_id']}),flush=True)
                    stale=[]
                    for candidate in candidates:
                        checked=candidate.get('url_checked_at')
                        if not checked or now-datetime.fromisoformat(checked)>=timedelta(days=7):
                            stale.append(candidate)
                    # Save each source-lookup batch before starting another batch.
                    for offset in range(0,len(stale),25):
                        client.capacity()
                        batch=stale[offset:offset+25]
                        fresh=lookup_candidate_urls(root,batch,limit=len(batch))
                        current={r['candidate_id']:r for r in fresh}
                        for candidate in batch:
                            updated=current[candidate['candidate_id']]
                            if updated.get('website_url') and updated.get('website_url')!=candidate.get('website_url'):
                                collect_live(store,client,updated,now.date().isoformat())
                        candidates=[dict(c,**current[c['candidate_id']]) for c in candidates]
                        if pilot:
                            write_json(root/'pilot_roster.json',candidates)
                        print(json.dumps({'phase':'url_refresh','batch_size':len(batch)}),flush=True)
                if args.phase in {'archive','all'}:
                    for candidate in candidates:
                        collect_archive(store,client,candidate,events,now.strftime('%Y%m%d'))
                        print(json.dumps({'phase':'archive','candidate_id':candidate['candidate_id']}),flush=True)
            except RuntimeError as exc:
                stop_reason=str(exc)
                if stop_reason not in {'run_time_budget','storage_headroom'}:
                    raise
            except Exception as exc:
                stop_reason='error_'+type(exc).__name__
                raise
            finally:
                store.recover()
                report=export(root,roster(root,pilot),events,store)
                write_json(root/'run_status.json',dict(status='paused' if stop_reason else 'finished',reason=stop_reason,
                           phase=args.phase,pilot=pilot,finished_at=utcnow(),coverage=report))
            print(json.dumps({'status':'paused' if stop_reason else 'finished','reason':stop_reason}),flush=True)


if __name__=='__main__':
    main()
