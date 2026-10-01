#!/usr/bin/env python3
"""Audit saved capture inventories and responses without archive requests."""
import collections
import calendar
import csv
import datetime as dt
import gzip
import hashlib
import json
from pathlib import Path
import re
import sqlite3
import subprocess
import sys
from urllib.parse import urlsplit, urlunsplit

INPUT = Path('data/municipal_discovery/exhaustive_source_expansion_20260908/ai_review_20260914/candidate_urls.csv')
OUT = Path('quality_reports/mayoral_capture_completeness_2026-09-14')


def rows(path):
    with Path(path).open(newline='') as stream:
        yield from csv.DictReader(stream)


def sha(path):
    h = hashlib.sha256()
    with Path(path).open('rb') as stream:
        for block in iter(lambda: stream.read(1048576), b''):
            h.update(block)
    return h.hexdigest()


def exact(url):
    p = urlsplit(url)
    return urlunsplit((p.scheme.lower(), p.netloc.lower(), p.path or '/', p.query, ''))


def relation(url, original):
    if exact(url) == exact(original):
        return 'exact'
    a, b = urlsplit(url), urlsplit(original)
    def variant_key(p):
        port = None if (p.scheme.lower(), p.port) in {('http',80),('https',443)} else p.port
        return ((p.hostname or '').removeprefix('www.'), port, p.path or '/', p.query)
    if variant_key(a) == variant_key(b):
        return 'scheme_www_variant'
    return 'domain_other_path_or_host'


def _exact_capture_key(row):
    identity = {k: row[k] for k in ('candidate_year_id', 'timestamp', 'original_url')}
    identity['provider'] = 'web.archive.org'
    return hashlib.sha256(json.dumps(identity, sort_keys=True).encode()).hexdigest()


def merge_exact_batch(remote, batch_root):
    """Verify and merge one completed frozen exact-capture acquisition batch."""
    root = Path(batch_root)
    frozen = root/'inputs'
    queue_path, manifest_path, ledger_path = (frozen/'exact_capture_queue.csv',
        frozen/'manifest.json', root/'run'/'ledger.sqlite3')
    manifest = json.loads(manifest_path.read_text())
    if sha(queue_path) != manifest['output_sha256']:
        raise ValueError('exact batch queue hash differs from frozen manifest')
    queue = list(rows(queue_path))
    expected_count = int(manifest['selected_capture_keys'])
    if len(queue) != expected_count:
        raise ValueError('exact batch queue row count differs from manifest')
    queue_by_key = {}
    for row in queue:
        key = _exact_capture_key(row)
        if key != row['proposed_capture_key'] or key in queue_by_key:
            raise ValueError('duplicate or invalid exact batch queue key')
        queue_by_key[key] = row
    uri = 'file:' + str(ledger_path.resolve()) + '?mode=ro'
    with sqlite3.connect(uri, uri=True) as db:
        db.row_factory = sqlite3.Row
        current_rows = list(db.execute('SELECT key,status,result FROM current'))
        contract_rows = list(db.execute('SELECT value FROM contract'))
        attempts = {r['key']: r for r in db.execute(
            "SELECT key,started,finished,result FROM attempts WHERE status='fetched' ORDER BY finished")}
    if len(contract_rows) != 1:
        raise ValueError('exact batch ledger contract missing or duplicated')
    contract = json.loads(contract_rows[0]['value'])
    if contract.get('queue_sha256') != manifest['output_sha256']:
        raise ValueError('exact batch ledger queue contract differs')
    current = {r['key']: r for r in current_rows}
    if len(current) != len(current_rows) or set(current) != set(queue_by_key):
        raise ValueError('exact batch current-state keys differ from frozen queue')
    if len(current) != expected_count or any(r['status'] != 'fetched' for r in current_rows):
        raise ValueError('exact batch is not completely fetched')
    captures = {}
    for capture in remote['captures']:
        key = (capture['candidate_year_id'], capture['timestamp'], exact(capture['original_url']))
        if key in captures:
            raise ValueError('duplicate upstream capture identity')
        captures[key] = capture
    additions = []
    checked = verified = 0
    for key, queue_row in queue_by_key.items():
        identity = (queue_row['candidate_year_id'], queue_row['timestamp'], exact(queue_row['original_url']))
        if identity not in captures:
            raise ValueError('exact batch key has no upstream capture')
        if key not in attempts:
            raise ValueError('exact batch fetched attempt missing')
        result = json.loads(current[key]['result'])
        if (result.get('requested_timestamp') != queue_row['timestamp']
                or result.get('requested_original_url') != queue_row['original_url']
                or result.get('effective_timestamp') != queue_row['timestamp']
                or result.get('effective_original_url') != queue_row['original_url']):
            raise ValueError('exact batch result identity mismatch')
        redirects = result.get('redirects') or []
        if not redirects or any(hop.get('oversized') for hop in redirects):
            raise ValueError('exact batch result lacks complete raw evidence')
        effective = urlsplit(result.get('effective_replay_url', ''))
        match = re.fullmatch(r'/web/(\d{14})(?:id_)?/(https?://.+)', effective.path)
        replay_original = match[2] + (('?' + effective.query) if effective.query else '') if match else ''
        if (effective.scheme != 'https' or effective.hostname != 'web.archive.org'
                or not match or (match[1], replay_original) != (queue_row['timestamp'], queue_row['original_url'])):
            raise ValueError('exact batch effective replay URL mismatch')
        if redirects[-1].get('status_code') != 200:
            raise ValueError('exact batch final response is not HTTP 200')
        for raw_hop in redirects:
            raw_evidence_path = root/'run'/raw_hop['raw_path']
            checked += 1
            try:
                raw_evidence = gzip.decompress(raw_evidence_path.read_bytes())
            except (OSError, EOFError, KeyError) as exc:
                raise ValueError('exact batch evidence file missing or invalid') from exc
            if (len(raw_evidence) != raw_hop['decoded_bytes']
                    or hashlib.sha256(raw_evidence).hexdigest() != raw_hop['raw_sha256']):
                raise ValueError('exact batch evidence hash or byte count mismatch')
            verified += 1
        hop = redirects[-1]
        raw_path = root/'run'/hop['raw_path']
        text_path = root/'run'/result['text_path']
        try:
            raw = gzip.decompress(raw_path.read_bytes())
            text = text_path.read_bytes()
        except (OSError, EOFError, KeyError) as exc:
            raise ValueError('exact batch evidence file missing or invalid') from exc
        valid = (len(raw) == hop['decoded_bytes']
                 and hashlib.sha256(raw).hexdigest() == hop['raw_sha256']
                 and len(text) == result['text_bytes']
                 and len(text) > 0 and hashlib.sha256(text).hexdigest() == result['text_sha256'])
        if not valid:
            raise ValueError('exact batch evidence hash or byte count mismatch')
        capture = captures[identity]
        if ('exact_capture_20260914' in capture['queue_sources']
                or any(f.get('batch') == 'exact_capture_20260914' or f.get('capture_id') == key
                       for f in capture['fetches'])):
            raise ValueError('exact batch fetch was already merged')
        finished = attempts[key]['finished'] or attempts[key]['started']
        attempted_at = dt.datetime.fromtimestamp(finished, dt.timezone.utc).isoformat()
        fetch = {
            'capture_id': key, 'candidate_year_id': queue_row['candidate_year_id'],
            'timestamp': queue_row['timestamp'], 'original_url': queue_row['original_url'],
            'fetch_status': 'fetched', 'retryable': False,
            'attempted_at_utc': attempted_at, 'raw_html_path': hop['raw_path'],
            'raw_html_sha256': hop['raw_sha256'], 'raw_html_bytes': hop['decoded_bytes'],
            'text_path': result['text_path'], 'text_sha256': result['text_sha256'],
            'text_bytes': result['text_bytes'], 'batch': 'exact_capture_20260914',
            'raw_absolute_path': str(raw_path), 'raw_verified': True,
            'compressed_bytes': raw_path.stat().st_size}
        additions.append((capture, fetch))
    for capture, fetch in additions:
        capture['queue_sources'].append('exact_capture_20260914')
        capture['fetches'].append(fetch)
    remote['raw_files_checked'] += checked
    remote['raw_files_verified'] += verified
    remote['hashes'][str(queue_path)] = sha(queue_path)
    remote['hashes'][str(ledger_path)] = sha(ledger_path)
    return remote


def scan(requests):
    base = Path('/root/candidate_website_extension/data/municipal_archive_acquisition')
    full = Path('/mnt/mayoral_wayback_full_snapshots/runs/production_all_inventory')
    names = ['covered_151_20260826', 'openai_residual_100_20260826', 'openai_residual_expansion_357_20260826', 'full_discovery_740_20260826']
    batches = [(n, base/n/'runs/production', base/n/'frozen/targets.csv', base/n/'runs/production') for n in names]
    batches.append(('full_corpus', full, base/names[-1]/'frozen/targets.csv', base/names[-1]/'runs/production'))
    pairs = {(r['candidate_year_id'], r['registrable_domain']) for r in requests}
    queries, target_rows, capture_rows, hashes = [], [], {}, {}
    checks = collections.Counter()
    checked = {}
    for name, root, frozen, inventory_root in batches:
        targets = {r['target_id']: r for r in rows(frozen) if (r['candidate_year_id'], r['registrable_domain']) in pairs}
        hashes[str(frozen)] = sha(frozen)
        for t in targets.values():
            target_rows.append(dict(t, batch=name))
        if name != 'full_corpus':
            query_path = inventory_root/'inventory_variant_attempts.csv'
            hashes[str(query_path)] = sha(query_path)
            for q in rows(query_path):
                if q['target_id'] not in targets:
                    continue
                p = inventory_root/q['raw_response_path']
                q = dict(q, batch=name, raw_query_path=str(p), raw_hash_verified=False, raw_data_rows=-1)
                if p.is_file():
                    raw = gzip.decompress(p.read_bytes())
                    q['raw_hash_verified'] = hashlib.sha256(raw).hexdigest() == q['raw_response_sha256']
                    payload = json.loads(raw)
                    q['raw_data_rows'] = max(0, len(payload)-1) if isinstance(payload, list) else -1
                queries.append(q)
        stage_rows = {}
        for filename, stage in [('capture_inventory.csv','inventory'), ('fetch_queue.csv','queue'), ('fetch_attempts.csv','fetch'), ('fetch_attempts.journal.csv','fetch')]:
            path = root/filename
            if not path.exists():
                continue
            hashes[str(path)] = sha(path)
            seen = set()
            for c in rows(path):
                if c['target_id'] not in targets:
                    continue
                checks[name+'_'+filename+'_duplicate_keys'] += c['capture_id'] in seen
                seen.add(c['capture_id'])
                key = c['candidate_year_id'], c['timestamp'], exact(c['original_url'])
                item = capture_rows.setdefault(key, {'candidate_year_id': key[0], 'timestamp': key[1], 'original_url': key[2], 'registrable_domain': targets[c['target_id']]['registrable_domain'], 'inventory_sources': [], 'queue_sources': [], 'fetches': []})
                if stage != 'fetch':
                    item[stage+'_sources'].append(name)
                else:
                    stage_rows[c['capture_id']] = c
            checks[name+'_'+filename+'_unique_keys'] = len(seen)
        for c in stage_rows.values():
            key = c['candidate_year_id'], c['timestamp'], exact(c['original_url'])
            p = root/c['raw_html_path'] if c['raw_html_path'] else None
            verified = False
            if p and p.is_file():
                ck = str(p), c['raw_html_sha256']
                if ck not in checked:
                    try:
                        raw = gzip.decompress(p.read_bytes())
                        checked[ck] = (hashlib.sha256(raw).hexdigest() == c['raw_html_sha256'], len(raw), p.stat().st_size)
                    except (OSError, EOFError):
                        checked[ck] = (False, 0, 0)
                verified = checked[ck][0]
            capture_rows[key]['fetches'].append({**c, 'batch': name, 'raw_absolute_path': str(p or ''), 'raw_verified': verified, 'compressed_bytes': checked.get((str(p), c['raw_html_sha256']), (False, 0, 0))[2]})
    remote = {'targets': target_rows, 'queries': queries, 'captures': list(capture_rows.values()), 'hashes': hashes, 'key_checks': checks, 'raw_files_checked': len(checked), 'raw_files_verified': sum(v[0] for v in checked.values())}
    exact_root = Path('/mnt/mayoral_wayback_full_snapshots/exact_capture_20260914')
    return merge_exact_batch(remote, exact_root) if exact_root.exists() else remote


def write_csv(path, data):
    data = list(data)
    fields = list(dict.fromkeys(k for r in data for k in r)) or ['candidate_url_id']
    with path.open('w', newline='') as stream:
        writer = csv.DictWriter(stream, fieldnames=fields)
        writer.writeheader()
        writer.writerows(data)


def summarize(requests, remote):
    audit, retry, query_retry = [], [], []
    for r in requests:
        matched = [t for t in remote['targets'] if (t['candidate_year_id'],t['registrable_domain']) == (r['candidate_year_id'],r['registrable_domain'])]
        tids = {(t['batch'],t['target_id']) for t in matched}
        qs = [q for q in remote['queries'] if (q['batch'],q['target_id']) in tids]
        expected = {(t['batch'], t['target_id'], u, t['scrape_window_start'].replace('-',''),t['scrape_window_end'].replace('-','')) for t in matched if t['batch'] != 'full_corpus' for u in json.loads(t['url_variants_json'])}
        good = {(q['batch'],q['target_id'],q['variant_url'],q['window_start'],q['window_end']) for q in qs if q['inventory_status'] in {'found','no_capture'} and q['raw_hash_verified'] and 0 <= q['raw_data_rows'] < 50000}
        counts = collections.Counter({rel+'_'+field: 0 for rel in ['exact','scheme_www_variant','domain_other_path_or_host'] for field in ['inventory','queue','verified_saved','saved_no_content','raw_bytes','compressed_bytes','timestamp_mismatch','unsaved']})
        for c in remote['captures']:
            if (c['candidate_year_id'],c['registrable_domain']) != (r['candidate_year_id'],r['registrable_domain']):
                continue
            rel = relation(r['exact_normalized_url'], c['original_url'])
            counts[rel+'_inventory'] += bool(c['inventory_sources'])
            counts[rel+'_queue'] += bool(c['queue_sources'])
            saved = [f for f in c['fetches'] if f['raw_verified']]
            counts[rel+'_verified_saved'] += bool(saved)
            if saved:
                counts[rel+'_raw_bytes'] += int(saved[-1]['raw_html_bytes'])
                counts[rel+'_compressed_bytes'] += saved[-1]['compressed_bytes']
                counts[rel+'_timestamp_mismatch'] += all(f['fetch_status'] == 'fetched_timestamp_mismatch' for f in saved)
                counts[rel+'_saved_no_content'] += all(f['fetch_status'] == 'no_content' for f in saved)
            elif c['inventory_sources']:
                fs = sorted(c['fetches'], key=lambda x: x['attempted_at_utc'])
                last = fs[-1] if fs else {}
                reason = 'not_queued' if not c['queue_sources'] else 'never_attempted' if not fs else 'missing_or_invalid_saved_file' if last.get('fetch_status') in {'fetched','fetched_timestamp_mismatch'} else last.get('fetch_status','unknown')
                counts[rel+'_unsaved'] += 1
                retry.append({k:r[k] for k in ['candidate_url_id','candidate_year_id','exact_normalized_url']} | {'url_relation':rel,'timestamp':c['timestamp'],'original_url':c['original_url'],'reason':reason,'next_action':'fetch_never_queued_capture' if reason == 'not_queued' else 'review_terminal_no_content' if reason == 'no_content' else 'retry_fetch','last_retryable':last.get('retryable',''),'last_capture_id':last.get('capture_id',''),'inventory_sources_json':json.dumps(c['inventory_sources'])})
        missing = expected-good
        state = 'not_in_saved_target_inventories' if not matched else 'saved_query_gap_or_cap' if missing else 'saved_filtered_queries_terminal'
        row = {k:r[k] for k in ['candidate_url_id','candidate_year_id','full_name','exact_normalized_url','registrable_domain']}
        row.update(inventory_state=state, expected_query_keys=len(expected), verified_terminal_query_keys=len(expected & good), query_gap_keys=len(missing), inventory_windows_json=json.dumps(sorted({(t['scrape_window_start'],t['scrape_window_end']) for t in matched})), **counts)
        row['capture_completeness_status'] = 'not_inventoried_in_checked_batches' if not matched else 'known_inventory_captures_not_saved' if any(counts[rel+'_unsaved'] for rel in ['exact','scheme_www_variant','domain_other_path_or_host']) else 'all_saved_inventory_responses_verified_including_no_content'
        audit.append(row)
        if not matched or missing:
            window = {}
            if r.get('year') and r.get('month'):
                year, month = int(float(r['year'])), int(float(r['month']))
                offset = year*12 + month-1-18
                start = dt.date(offset//12,offset%12+1,1)
                end = dt.date(year,month,calendar.monthrange(year,month)[1])+dt.timedelta(days=120)
                window = {'proposed_window_start':start.isoformat(),'proposed_window_end':end.isoformat(),'window_rule':'existing_freeze_rule_18_months_before_election_month_to_120_days_after_month_end'}
            query_retry.append({**row,**window,'action':'inventory_accepted_exact_url_and_explicit_variants' if not matched else 'retry_missing_or_capped_query_keys','expected_capture_count':'unknown_before_inventory','missing_query_keys_json':json.dumps(sorted(missing))})
    return audit, retry, query_retry


def main():
    requests = [r for r in rows(INPUT) if r['ai_accepted_website'].lower() == 'true']
    assert len(requests) == 41 and len({r['candidate_url_id'] for r in requests}) == 41
    OUT.mkdir(parents=True, exist_ok=True)
    source = Path(__file__).read_text().split('\ndef main():')[0]
    if '--reuse-remote' in sys.argv:
        previous = json.loads((OUT/'summary.json').read_text())
        assert previous['input_sha256'] == sha(INPUT), 'Cached input differs'
        remote = json.loads((OUT/'remote_evidence.json').read_text())
    else:
        result = subprocess.run(['ssh','-o','BatchMode=yes','-o','ConnectTimeout=15','root@droplet','python3','-'],input=source+'\nprint(json.dumps(scan('+repr(requests)+')))\n',text=True,capture_output=True,timeout=300)
        if result.returncode:
            raise RuntimeError(result.stderr)
        remote = json.loads(result.stdout)
    (OUT/'remote_evidence.json').write_text(json.dumps(remote,indent=2)+'\n')
    audit,retry,query_retry = summarize(requests,remote)
    write_csv(OUT/'url_capture_audit.csv',audit)
    write_csv(OUT/'targeted_capture_retry.csv',retry)
    write_csv(OUT/'targeted_inventory_queue.csv',query_retry)
    write_csv(OUT/'query_evidence.csv',remote['queries'])
    no_content = [c for c in remote['captures'] if c['fetches'] and all(f['fetch_status'] == 'no_content' for f in c['fetches'])]
    write_csv(OUT/'saved_no_content_review.csv',[{k:c[k] for k in ['candidate_year_id','timestamp','original_url','registrable_domain']} | {'fetch_evidence_json':json.dumps(c['fetches'])} for c in no_content])
    successes = [next(f for f in reversed(c['fetches']) if f['raw_verified']) for c in remote['captures'] if any(f['raw_verified'] for f in c['fetches'])]
    mean_raw = sum(int(f['raw_html_bytes']) for f in successes)/max(1,len(successes))
    mean_gz = sum(f['compressed_bytes'] for f in successes)/max(1,len(successes))
    unique_retry = {(r['candidate_year_id'],r['timestamp'],r['original_url']) for r in retry}
    summary = {'input':str(INPUT),'input_sha256':sha(INPUT),'script_sha256':sha(Path(__file__)),'accepted_urls':len(audit),'candidate_years':len({r['candidate_year_id'] for r in audit}),'inventory_states':dict(collections.Counter(r['inventory_state'] for r in audit)),'retry_rows_by_relation':dict(collections.Counter(r['url_relation'] for r in retry)),'unique_unsaved_capture_keys':len(unique_retry),'inventory_queue_urls':len(query_retry),'observed_verified_capture_keys':len(successes),'mean_raw_response_bytes':mean_raw,'mean_compressed_response_bytes':mean_gz,'estimated_raw_bytes_for_known_unsaved':mean_raw*len(unique_retry),'estimated_compressed_bytes_for_known_unsaved':mean_gz*len(unique_retry),'raw_files_checked':remote['raw_files_checked'],'raw_files_verified':remote['raw_files_verified'],'limitations':['Original queries use prefix, statuscode:200, mimetype:text/html, collapse:digest, and limit:50000 without pagination. Terminal status describes this filtered inventory only.','All historical timestamps require a separate inventory without digest collapse.','New inventory capture counts and bytes remain unknown. Known retry estimates use the mean saved response size and exclude text and metadata.','Scheme/www variants and other paths do not prove exact URL equivalence or campaign ownership.','Fetch ledgers preserve current state, not unique execution attempt IDs. Request counts are not attempt-journal counts.','Timestamp mismatch responses remain saved evidence but require separate date review.','No archive requests or remote writes occurred.']}
    summary['capture_actions'] = dict(collections.Counter(r['next_action'] for r in retry))
    summary['saved_no_content_capture_keys'] = len(no_content)
    summary['capture_key_definition'] = 'candidate_year_id + requested timestamp + exact original URL; deduplicated across batches'
    summary['byte_estimates_by_relation'] = {rel: {'capture_rows':count,'raw_bytes':mean_raw*count,'compressed_bytes':mean_gz*count} for rel,count in collections.Counter(r['url_relation'] for r in retry if r['next_action'] == 'fetch_never_queued_capture').items()}
    raw_sizes = sorted(int(f['raw_html_bytes']) for f in successes)
    gzip_sizes = sorted(f['compressed_bytes'] for f in successes)
    summary['illustrative_size_scenarios'] = {label: {'observed_raw_bytes_per_capture':raw_sizes[int((len(raw_sizes)-1)*p)],'observed_compressed_bytes_per_capture':gzip_sizes[int((len(gzip_sizes)-1)*p)],'all_known_pending_raw_bytes':raw_sizes[int((len(raw_sizes)-1)*p)]*len(unique_retry),'all_known_pending_compressed_bytes':gzip_sizes[int((len(gzip_sizes)-1)*p)]*len(unique_retry)} for label,p in [('p25',.25),('p50',.5),('p75',.75)]} if successes else {}
    summary['size_estimate_caveat'] = 'Quartile scenarios describe observed saved response sizes, not confidence intervals or upper bounds. New captures can differ. Bytes for 22 new inventories remain unknown.'
    (OUT/'summary.json').write_text(json.dumps(summary,indent=2)+'\n')
    print(json.dumps(summary,indent=2))


if __name__ == '__main__':
    main()
