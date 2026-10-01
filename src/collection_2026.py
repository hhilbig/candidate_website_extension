"""Acquire dated campaign pages with a durable request and work ledger."""
from __future__ import annotations

import contextlib
import gzip
import hashlib
import ipaddress
import json
import os
import re
import shutil
import socket
import sqlite3
import tempfile
import time
import unicodedata
import uuid
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from urllib.parse import urljoin, urlsplit, urlunsplit

import requests
from bs4 import BeautifulSoup

from src.extract_text import classify_page_type, extract_visible_text

VERSION = 'collection-2026-v1'
RETRYABLE = {408, 425, 429, 500, 502, 503, 504}
PAGE_TYPES = {'homepage', 'issues', 'biography', 'endorsements', 'news'}
MEDIA = re.compile(r'\.(pdf|jpe?g|png|gif|webp|svg|mp[34]|zip|css|js|ico|woff2?|ttf)(?:$|\?)', re.I)
EXCLUDED = {'facebook.com', 'twitter.com', 'x.com', 'instagram.com', 'youtube.com',
            'secure.actblue.com', 'actblue.com', 'winred.com', 'secure.winred.com'}
REPLAY = re.compile(r'^https?://web\.archive\.org/web/(\d{14})[^/]*/(https?://.+)$')


def utcnow():
    return datetime.now(timezone.utc).isoformat()


def stable(*parts):
    return hashlib.sha256(json.dumps(parts, sort_keys=True, default=str).encode()).hexdigest()[:32]


def atomic(path: Path, content: bytes):
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(dir=path.parent, delete=False) as f:
        tmp = Path(f.name)
        f.write(content)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)


def write_json(path: Path, value):
    atomic(path, (json.dumps(value, indent=2, sort_keys=True, default=str) + '\n').encode())


def clean_url(url):
    try:
        p = urlsplit(str(url).strip())
        if p.scheme not in {'http', 'https'} or not p.hostname or p.username or p.password:
            return ''
        if p.port not in {None, 80, 443} or any(p.hostname.lower()==d or p.hostname.lower().endswith('.'+d) for d in EXCLUDED):
            return ''
        return urlunsplit((p.scheme, p.netloc.lower(), p.path or '/', p.query, ''))
    except ValueError:
        return ''


def host(url):
    return (urlsplit(url).hostname or '').removeprefix('www.').lower()


def public_url(url):
    if not clean_url(url):
        return False
    try:
        addresses = socket.getaddrinfo(urlsplit(url).hostname, None)
        return bool(addresses) and all(ipaddress.ip_address(a[4][0]).is_global for a in addresses)
    except (OSError, ValueError):
        return False


def original_url(url):
    m = REPLAY.match(url)
    return m.group(2) if m else url


def timestamp_iso(value):
    return datetime.strptime(value, '%Y%m%d%H%M%S').replace(tzinfo=timezone.utc).isoformat()


def identity_match(candidate, text):
    def words(value):
        value = unicodedata.normalize('NFKD', value).encode('ascii', 'ignore').decode().lower()
        return set(re.findall(r'[a-z]+', value))
    raw = candidate.get('fec_raw_name', '')
    if ',' in raw:
        surname, given = raw.split(',', 1)
        surname_words = words(surname) - {'jr', 'sr', 'ii', 'iii', 'iv'}
        given_words = words(given) - {'jr', 'sr', 'ii', 'iii', 'iv', 'mr', 'mrs', 'dr'}
    else:
        tokens = re.findall(r'[A-Za-z]+', candidate.get('candidate', ''))
        surname_words = words(tokens[-1]) if tokens else set()
        given_words = words(tokens[0]) if len(tokens) > 1 else set()
    observed = words(text)
    return bool(surname_words and surname_words <= observed and given_words & observed)


# A MIME-only server filter would also remove redirects with misleading MIME.
# Preserve broad compressed CDX audits and filter the working table locally.
CDX_ASSET_MIME_FILTER = None
CAPTURE_IDENTITY_VERSION = 'candidate_timestamp_original_v1'


def capture_omission_reason(row):
    """Keep possible HTML responses; exclude known assets without fetching them."""
    from urllib.parse import unquote
    url = str(row.get('original', ''))
    try:
        path = unquote(urlsplit(url).path)
    except ValueError:
        return 'invalid_original_url'
    if MEDIA.search(path) or re.search(
            r'\.(avif|bmp|tiff?|heic|webm|mov|avi|m4[av]|wav|ogg|ogv|flac|aac|'
            r'woff2?|eot|otf|map|json|xml|txt|csv|webmanifest|gz|rar|7z|docx?|xlsx?|pptx?)(?:$|/)', path, re.I):
        return 'known_asset_url'
    if str(row.get('statuscode', '')) in {'301','302','303','307','308'}:
        return ''
    mime = str(row.get('mimetype') or '').split(';', 1)[0].strip().lower()
    if 'html' in mime or mime in {'', '-', 'unk', 'unknown', 'warc/revisit',
                                 'application/octet-stream', 'binary/octet-stream'}:
        return ''
    return 'known_non_html_mimetype'


class Store:
    def __init__(self, root):
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(self.root / 'collection.sqlite')
        self.db.row_factory = sqlite3.Row
        self.db.executescript('''
            PRAGMA journal_mode=WAL;
            CREATE TABLE IF NOT EXISTS work (
                key TEXT PRIMARY KEY, kind TEXT, candidate_id TEXT, event_id TEXT, side TEXT,
                payload TEXT, status TEXT, tries INTEGER DEFAULT 0, updated_at TEXT, result TEXT);
            CREATE TABLE IF NOT EXISTS attempts (
                attempt_id TEXT PRIMARY KEY, work_key TEXT, request_url TEXT,
                started_at TEXT, finished_at TEXT, status TEXT, detail TEXT);
            CREATE INDEX IF NOT EXISTS work_candidate ON work(candidate_id);
            CREATE INDEX IF NOT EXISTS attempt_work_lookup ON attempts(work_key);
            CREATE TABLE IF NOT EXISTS pages (page_id TEXT PRIMARY KEY, data TEXT);
            CREATE INDEX IF NOT EXISTS page_capture_lookup ON pages(
                json_extract(data,'$.candidate_id'),json_extract(data,'$.requested_timestamp'));
            CREATE TABLE IF NOT EXISTS captures (capture_id TEXT PRIMARY KEY, inventory_key TEXT, data TEXT);
            CREATE INDEX IF NOT EXISTS inventory_captures ON captures(inventory_key);
            CREATE INDEX IF NOT EXISTS capture_latest_inventory ON captures(json_extract(data,'$.last_inventory_key'));
            CREATE INDEX IF NOT EXISTS capture_complete_inventory ON captures(json_extract(data,'$.latest_complete_inventory_key'));
        ''')
        self.db.commit()

    def add_work(self, key, kind, candidate, payload, event_id='', side=''):
        self.db.execute('INSERT OR IGNORE INTO work VALUES(?,?,?,?,?,?,?,0,?,?)',
                        (key, kind, candidate, event_id, side, json.dumps(payload), 'pending', utcnow(), '{}'))
        self.db.commit()

    def update(self, key, status, result=None):
        self.db.execute('UPDATE work SET status=?,updated_at=?,result=? WHERE key=?',
                        (status, utcnow(), json.dumps(result or {}), key))
        self.db.commit()

    def start(self, key):
        self.db.execute("UPDATE work SET status='running',tries=tries+1,updated_at=? WHERE key=?", (utcnow(), key))
        self.db.commit()

    def attempt(self, key, url):
        aid = uuid.uuid4().hex
        self.db.execute('INSERT INTO attempts VALUES(?,?,?,?,?,?,?)', (aid, key, url, utcnow(), '', 'started', '{}'))
        self.db.commit()
        return aid

    def finish_attempt(self, aid, status, detail):
        self.db.execute('UPDATE attempts SET finished_at=?,status=?,detail=? WHERE attempt_id=?',
                        (utcnow(), status, json.dumps(detail), aid))
        self.db.commit()

    def rows(self, table):
        if table not in {'pages', 'captures', 'work', 'attempts'}:
            raise ValueError(table)
        rows = [dict(r) for r in self.db.execute(f'SELECT * FROM {table}')]
        if table in {'pages', 'captures'}:
            return [json.loads(r['data']) for r in rows]
        for row in rows:
            for key in ('payload', 'result', 'detail'):
                if key in row:
                    row[key] = json.loads(row[key])
        return rows

    def page(self, row):
        self.db.execute('INSERT OR REPLACE INTO pages VALUES(?,?)', (row['page_id'], json.dumps(row)))
        self.db.commit()

    def capture(self, key, row):
        reason = capture_omission_reason(row)
        if reason:
            return {'status': 'omitted', 'reason': reason}
        candidate_id = row.get('candidate_id')
        work = self.db.execute('SELECT candidate_id,status FROM work WHERE key=?', (key,)).fetchone()
        candidate_id = candidate_id or (work['candidate_id'] if work else '')
        # Unresolved legacy records remain isolated by inventory, never by name.
        scope = str(candidate_id) if candidate_id else 'unresolved_inventory:' + key
        cid = stable(CAPTURE_IDENTITY_VERSION, scope, row['timestamp'], row['original'])
        record = dict(row, capture_id=cid, inventory_key=key, last_inventory_key=key,
                      capture_identity_version=CAPTURE_IDENTITY_VERSION)
        if candidate_id:
            record['candidate_id'] = candidate_id
        if work and work['status'] in {'complete', 'empty'}:
            record['latest_complete_inventory_key'] = key
        inserted = self.db.execute('INSERT OR IGNORE INTO captures VALUES(?,?,?)',
                                   (cid, key, json.dumps(record))).rowcount
        if not inserted:
            self.db.execute("UPDATE captures SET data=json_set(data,'$.last_inventory_key',?) WHERE capture_id=?", (key, cid))
            if work and work['status'] in {'complete', 'empty'}:
                self.db.execute("UPDATE captures SET data=json_set(data,'$.latest_complete_inventory_key',?) WHERE capture_id=?", (key, cid))
        return {'status': 'inserted' if inserted else 'reused', 'capture_id': cid}

    def migrate_capture_inventory(self, *, dry_run=False):
        """Remove asset metadata and merge weekly repeats after an external backup.

        Raw response files, request attempts, pages, and work records stay intact.
        SQLite may need a separate VACUUM to return unused disk space.
        """
        from collections import Counter
        from functools import lru_cache
        if self.db.in_transaction:
            raise RuntimeError('Commit pending acquisition records before capture migration')
        before = self.db.execute('SELECT COUNT(*) FROM captures').fetchone()[0]
        omitted, duplicates, unresolved, kept = Counter(), 0, 0, 0

        @lru_cache(maxsize=1024)
        def source_work(key):
            row = self.db.execute('SELECT candidate_id,status FROM work WHERE key=?', (key,)).fetchone()
            return (row['candidate_id'], row['status']) if row else ('', '')

        self.db.execute('SAVEPOINT capture_inventory_migration')
        try:
            self.db.execute('CREATE TABLE captures_migration_v2 (capture_id TEXT PRIMARY KEY, inventory_key TEXT, data TEXT)')
            for saved in self.db.execute('SELECT capture_id,inventory_key,data FROM captures ORDER BY rowid'):
                row = json.loads(saved['data'])
                reason = capture_omission_reason(row)
                if reason:
                    omitted[reason] += 1
                    continue
                key = saved['inventory_key']
                candidate_id, source_status = source_work(key)
                candidate_id = row.get('candidate_id') or candidate_id
                if not candidate_id:
                    unresolved += 1
                scope = str(candidate_id) if candidate_id else 'unresolved_inventory:' + key
                cid = stable(CAPTURE_IDENTITY_VERSION, scope, row['timestamp'], row['original'])
                record = dict(row, capture_id=cid, inventory_key=key,
                              last_inventory_key=row.get('last_inventory_key', key),
                              capture_identity_version=CAPTURE_IDENTITY_VERSION)
                if candidate_id:
                    record['candidate_id'] = candidate_id
                if cid != saved['capture_id']:
                    record.setdefault('original_capture_id', saved['capture_id'])
                if source_status in {'complete', 'empty'}:
                    record.setdefault('latest_complete_inventory_key', key)
                inserted = self.db.execute('INSERT OR IGNORE INTO captures_migration_v2 VALUES(?,?,?)',
                                           (cid, key, json.dumps(record))).rowcount
                if inserted:
                    kept += 1
                else:
                    duplicates += 1
                    self.db.execute("UPDATE captures_migration_v2 SET data=json_set(data,'$.last_inventory_key',?) WHERE capture_id=?",
                                    (record['last_inventory_key'], cid))
                    if record.get('latest_complete_inventory_key'):
                        self.db.execute("UPDATE captures_migration_v2 SET data=json_set(data,'$.latest_complete_inventory_key',?) WHERE capture_id=?",
                                        (record['latest_complete_inventory_key'], cid))
            if kept + duplicates + sum(omitted.values()) != before:
                raise RuntimeError('Capture migration did not account for every input record')
            report = dict(migration=CAPTURE_IDENTITY_VERSION, performed_at=utcnow(), dry_run=dry_run,
                          before_capture_rows=before, after_capture_rows=kept,
                          omitted_metadata_rows=sum(omitted.values()), omitted_by_reason=dict(omitted),
                          duplicate_inventory_rows=duplicates, unresolved_candidate_rows=unresolved,
                          raw_response_files_modified=0, page_rows_modified=0, work_rows_modified=0,
                          attempt_rows_modified=0, first_inventory_provenance_preserved=True,
                          vacuum_not_run=True, inventory_server_filter=CDX_ASSET_MIME_FILTER)
            if dry_run:
                self.db.execute('ROLLBACK TO capture_inventory_migration')
            else:
                self.db.execute('DROP TABLE captures')
                self.db.execute('ALTER TABLE captures_migration_v2 RENAME TO captures')
                self.db.execute('CREATE INDEX inventory_captures ON captures(inventory_key)')
                self.db.execute("CREATE INDEX capture_latest_inventory ON captures(json_extract(data,'$.last_inventory_key'))")
                self.db.execute("CREATE INDEX capture_complete_inventory ON captures(json_extract(data,'$.latest_complete_inventory_key'))")
            self.db.execute('RELEASE capture_inventory_migration')
        except BaseException:
            self.db.execute('ROLLBACK TO capture_inventory_migration')
            self.db.execute('RELEASE capture_inventory_migration')
            raise
        if not dry_run:
            write_json(self.root / 'migrations' / f'capture_inventory_{uuid.uuid4().hex}.json', report)
        return report

    def recover(self):
        self.db.execute("UPDATE work SET status='retryable' WHERE status='running'")
        self.db.execute("UPDATE attempts SET status='interrupted',finished_at=? WHERE status='started'", (utcnow(),))
        self.db.commit()

    def blob(self, content):
        digest = hashlib.sha256(content).hexdigest()
        path = self.root / 'html' / digest[:2] / f'{digest}.html.gz'
        if not path.exists():
            atomic(path, gzip.compress(content, mtime=0))
        return str(path.relative_to(self.root)), digest


class Client:
    def __init__(self, store, config):
        self.store, self.config = store, config
        self.session = requests.Session()
        self.session.headers['User-Agent'] = config['user_agent']
        self.last_request = 0.0
        self.deadline = time.monotonic() + config['max_run_seconds']

    def capacity(self):
        if time.monotonic() >= self.deadline:
            raise RuntimeError('run_time_budget')
        if shutil.disk_usage(self.store.root).free < self.config['minimum_free_bytes']:
            raise RuntimeError('storage_headroom')

    def get(self, url, work_key, archive=False, page=False):
        self.capacity()
        last = {'status': 'request_error', 'error': 'no_attempt', 'body': b'', 'url': url}
        for retry in range(self.config['request_attempts']):
            aid = self.store.attempt(work_key, url)
            current, history = url, []
            try:
                for hop in range(8):
                    if not public_url(current) or (archive and host(current) != 'web.archive.org'):
                        last = {'status': 'blocked', 'error': 'unsafe_or_external_redirect', 'body': b'', 'url': current}
                        break
                    delay = self.config['wayback_delay_seconds' if archive else 'live_delay_seconds']
                    time.sleep(max(0, delay - (time.monotonic() - self.last_request)))
                    self.last_request = time.monotonic()
                    with self.session.get(current, allow_redirects=False, stream=True,
                                          timeout=(self.config['connect_timeout_seconds'], self.config['read_timeout_seconds'])) as response:
                        if response.is_redirect:
                            history.append({'url': current, 'status': response.status_code, 'location': response.headers.get('Location', '')})
                            current = urljoin(current, response.headers.get('Location', ''))
                            continue
                        content_type=response.headers.get('Content-Type','').split(';')[0].strip().lower()
                        if page and content_type and content_type not in {'text/html','application/xhtml+xml'}:
                            excluded_status=('request_error' if response.status_code in RETRYABLE else
                                             'http_error' if response.status_code>=400 else 'non_html')
                            last=dict(status=excluded_status,body=b'',url=current,http_status=response.status_code,
                                      headers=dict(response.headers),redirects=history,fetched_at=utcnow(),
                                      error='excluded_non_html_payload')
                            break
                        body = bytearray()
                        oversize = False
                        for chunk in response.iter_content(65536):
                            body.extend(chunk)
                            if len(body) > self.config['max_response_bytes']:
                                oversize = True
                                break
                        last = dict(status='oversize' if oversize else 'ok', body=bytes(body), url=current,
                                    http_status=response.status_code, headers=dict(response.headers), redirects=history,
                                    fetched_at=utcnow(), decoded_content_encoding=True)
                        if page and not content_type and not re.search(
                                rb'<(?:!doctype\s+html|html|head|body|div|p\b|main|article|title)\b',bytes(body[:2048]),re.I):
                            last.update(status='non_html',body=b'',error='untyped_non_html_payload')
                    if last['http_status'] in RETRYABLE:
                        last['status'] = 'request_error'
                    elif last['http_status'] >= 400:
                        last['status'] = 'http_error'
                    break
                else:
                    last = {'status': 'blocked', 'error': 'redirect_loop_or_limit', 'body': b'', 'url': current}
                # Preserve failed service responses as evidence too.
                if last.get('body'):
                    last['body_path'], last['body_sha256'] = self.store.blob(last['body'])
                self.store.finish_attempt(aid, last['status'], {k:v for k,v in last.items() if k != 'body'})
            except (requests.RequestException, OSError) as exc:
                last = {'status': 'request_error', 'error': type(exc).__name__, 'body': b'', 'url': current}
                self.store.finish_attempt(aid, 'request_error', {k:v for k,v in last.items() if k != 'body'})
            if last['status'] != 'request_error':
                break
            if retry + 1 < self.config['request_attempts']:
                time.sleep(min(2 ** retry * 2, 8))
        return last


def parse_cdx(body):
    text = body.decode('utf-8-sig').strip()
    value, end = json.JSONDecoder().raw_decode(text)
    if not isinstance(value, list):
        raise ValueError('CDX response is not a list')
    if not value:
        return [], ''
    header = value[0]
    if not {'timestamp', 'original', 'statuscode', 'mimetype'} <= set(header):
        raise ValueError('CDX header incomplete')
    rows = []
    data_rows = value[1:]
    json_resume = ''
    # CDX JSON pagination appends an empty row and a one-cell resume-key row.
    # Some deployments instead put the continuation after the JSON document.
    if len(data_rows) >= 2 and data_rows[-2] == [] and len(data_rows[-1]) == 1:
        json_resume = str(data_rows[-1][0])
        data_rows = data_rows[:-2]
    for row in data_rows:
        if len(row) != len(header):
            raise ValueError('CDX row width differs')
        record = dict(zip(header, row))
        if not re.fullmatch(r'\d{14}', record['timestamp']):
            raise ValueError('CDX timestamp invalid')
        rows.append(record)
    suffix = text[end:].strip()
    suffix_resume = suffix.splitlines()[0].strip() if suffix else ''
    if json_resume and suffix_resume and json_resume != suffix_resume:
        raise ValueError('Conflicting CDX continuation keys')
    return rows, json_resume or suffix_resume


def inventory(store, client, candidate, url, through_date):
    from collections import Counter
    from urllib.parse import urlencode
    # Prefix without wildcard. Scheme-free host includes HTTP and HTTPS captures.
    prefix = urlsplit(url).netloc + '/'
    # Reuse successful inventories for seven days. Retry partial queries with their
    # original upper bound so a new date does not erase their attempt history.
    from datetime import date
    recent = []
    for existing in store.db.execute("SELECT key,payload,status FROM work WHERE kind='cdx_inventory' AND candidate_id=?",(candidate['candidate_id'],)):
        previous = json.loads(existing['payload'])
        end = previous.get('through_date', '')
        if previous.get('prefix') == prefix and previous.get('server_filter') == CDX_ASSET_MIME_FILTER and end and 0 <= (date.fromisoformat(through_date[:4]+'-'+through_date[4:6]+'-'+through_date[6:])-
                date.fromisoformat(end[:4]+'-'+end[4:6]+'-'+end[6:])).days < 7:
            recent.append((end,existing['key'],existing['status']))
    if recent:
        previous_end,previous_key,previous_status=max(recent)
        if previous_status in {'complete','empty'}:
            return previous_key
        through_date=previous_end
    key = stable(VERSION, 'cdx', candidate['candidate_id'], prefix, '20260101', through_date)
    payload = dict(url=url, prefix=prefix, through_date=through_date, server_filter=CDX_ASSET_MIME_FILTER,
                   inventory_scope='full_cdx_with_local_asset_filter', capture_identity_version=CAPTURE_IDENTITY_VERSION)
    store.add_work(key, 'cdx_inventory', candidate['candidate_id'], payload)
    row = store.db.execute('SELECT status,result FROM work WHERE key=?', (key,)).fetchone()
    if row['status'] in {'complete', 'empty'}:
        return key
    saved_progress=json.loads(row['result'])
    store.start(key)
    resume=saved_progress.get('resume_key','')
    seen={resume} if resume else set()
    count=int(saved_progress.get('count_partial',0)) if resume else 0
    eligible=int(saved_progress.get('eligible_capture_rows',0)) if resume else 0
    inserted=int(saved_progress.get('new_unique_capture_rows',0)) if resume else 0
    reused=int(saved_progress.get('reused_capture_rows',0)) if resume else 0
    omitted=Counter(saved_progress.get('omitted_by_reason',{})) if resume else Counter()

    def progress(**extra):
        return dict(count_partial=count, returned_cdx_rows=count, eligible_capture_rows=eligible,
                    new_unique_capture_rows=inserted, reused_capture_rows=reused,
                    omitted_metadata_rows=sum(omitted.values()), omitted_by_reason=dict(omitted),
                    server_filter=CDX_ASSET_MIME_FILTER, server_excluded_row_count=None,
                    inventory_scope='full_cdx_with_local_asset_filter', **extra)

    while True:
        params = dict(url=prefix, matchType='prefix', output='json',
                      **{'from': '20260101', 'to': through_date},
                      fl='timestamp,original,statuscode,mimetype,digest,length',
                      showResumeKey='true', limit=10000)
        if resume:
            params['resumeKey'] = resume
        response = client.get('https://web.archive.org/cdx/search/cdx?' + urlencode(params), key, archive=True)
        if response['status'] != 'ok':
            store.update(key, 'retryable', progress(reason=response['status'], resume_key=resume))
            return key
        try:
            records, next_key = parse_cdx(response['body'])
            for record in records:
                result=store.capture(key, dict(record, candidate_id=candidate['candidate_id']))
                if result['status']=='omitted':
                    omitted[result['reason']]+=1
                else:
                    eligible+=1
                    inserted+=result['status']=='inserted'
                    reused+=result['status']=='reused'
            store.db.commit()
            count += len(records)
            if not next_key:
                break
            if next_key in seen:
                raise ValueError('repeated CDX continuation')
            seen.add(next_key)
            resume = next_key
            store.update(key,'running',progress(resume_key=resume))
        except (ValueError, UnicodeError) as exc:
            store.update(key, 'retryable', progress(reason=str(exc), resume_key=resume))
            return key
    # Records first seen during an incomplete query become eligible only when a
    # full query has observed them. The first inventory key remains unchanged.
    store.db.execute("UPDATE captures SET data=json_set(data,'$.latest_complete_inventory_key',?) WHERE json_extract(data,'$.last_inventory_key')=?", (key,key))
    store.update(key, 'complete' if count else 'empty', progress(capture_rows=count))
    for old in store.db.execute("SELECT key,payload,status FROM work WHERE kind='cdx_inventory' AND candidate_id=?",(candidate['candidate_id'],)).fetchall():
        payload=json.loads(old['payload'])
        if (old['key']!=key and old['status'] in {'retryable','failed','running'} and
                payload.get('prefix')==prefix and payload.get('through_date','99999999')<=through_date):
            store.update(old['key'],'superseded',{'superseded_by':key,'reason':'completed_wider_inventory'})
    return key


def page_links(html, base, source, max_depth, parent_type='homepage'):
    soup = BeautifulSoup(html, 'lxml')
    links = []
    for a in soup.find_all('a', href=True):
        # A malformed href, such as an unbalanced IPv6 bracket, raises
        # ValueError in urllib. One bad link on candidate S6GA00358 stopped every
        # daily run from 2026-09-24 to 2026-09-30, so skip the link instead.
        try:
            href = urljoin(original_url(base), a['href'])
            url = clean_url(original_url(href))
            if not url or host(url) != host(original_url(base)) or MEDIA.search(url):
                continue
            urlsplit(url)
        except ValueError:
            continue
        page_type = classify_page_type(url)
        label = (a.get_text(' ', strip=True) + ' ' + urlsplit(url).path).lower()
        if page_type not in PAGE_TYPES or (page_type=='homepage' and urlsplit(url).query):
            for ptype, names in [('issues', ['issues','priorities','platform','policy']), ('biography',['about','meet','biography']),
                                  ('endorsements',['endorsement','supporters']), ('news',['news','press','blog'])]:
                if any(name in label for name in names):
                    page_type = ptype
                    break
        excluded_path = re.search(r'/(contact|privacy|terms|login|donate|contribute|volunteer|events?)(/|$)',urlsplit(url).path,re.I)
        if page_type=='other' and parent_type in PAGE_TYPES-{'homepage'} and not excluded_path:
            page_type=parent_type
        if page_type in PAGE_TYPES:
            links.append((url, page_type))
    return sorted(set(links), key=lambda x: (['homepage','issues','biography','endorsements','news'].index(x[1]), x[0]))


def render_live(url, config):
    """Use a fresh credential-free browser context; never render archive scripts."""
    try:
        from playwright.sync_api import sync_playwright
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(accept_downloads=False, service_workers='block', user_agent=config['user_agent'])
            def route(req):
                request = req.request
                if request.resource_type in {'image', 'media', 'font'} or not public_url(request.url):
                    req.abort()
                else:
                    req.continue_()
            context.route('**/*', route)
            page = context.new_page()
            response = page.goto(url, wait_until='domcontentloaded', timeout=config['render_timeout_ms'])
            page.wait_for_timeout(1500)
            result = dict(body=page.content().encode(), url=page.url, fetched_at=utcnow(),
                          http_status=response.status if response else 0, status='ok', headers={}, rendered=True)
            browser.close()
            return result
    except Exception as exc:
        return {'status': 'request_error', 'error': 'render_' + type(exc).__name__}


def acquire_page(store, client, candidate, snap_id, url, page_type, depth, source,
                 work_key, event=None, side='', inherited_identity=False):
    from src.collection_2026_outputs import capture_side
    response = client.get(url, work_key, archive=source == 'wayback', page=True)
    fetched_at = response.get('fetched_at', utcnow())
    requested_match = REPLAY.match(url)
    effective_match = REPLAY.match(response['url'])
    stamp = effective_match.group(1) if effective_match else ''
    captured_at = timestamp_iso(stamp) if source == 'wayback' and stamp else fetched_at
    headers={k.lower():v for k,v in response.get('headers',{}).items()}
    memento=headers.get('memento-datetime','')
    memento_used=False
    if source=='wayback' and memento:
        try:
            parsed=parsedate_to_datetime(memento)
            if parsed.tzinfo:
                captured_at=parsed.astimezone(timezone.utc).isoformat()
                memento_used=True
        except (TypeError,ValueError):
            pass
    row = dict(page_id=stable(snap_id, url, page_type), candidate_id=candidate['candidate_id'],
               snapshot_id=snap_id, source=source, original_url=original_url(url),
               effective_url=response['url'], requested_timestamp=requested_match.group(1) if requested_match else '',
               captured_at=captured_at, fetched_at=fetched_at, page_type=page_type, depth=depth,
               status=response['status'], identity_status='unresolved', html_path=response.get('body_path',''),
               text_path='',sha256=response.get('body_sha256',''), n_char=0,
               requested_event_id=event['event_id'] if event else '', requested_side=side,
               event_date=event['date'] if event else '', error=response.get('error',''), render_used=False,
               http_status=response.get('http_status'), snapshot_complete=False,
               compressed_bytes=0, response_bytes=len(response.get('body', b'')))
    row.update(effective_url_timestamp=stamp,memento_datetime=memento,
               timestamp_source='memento_header' if memento_used else ('replay_url' if source=='wayback' else 'fetch_time'))
    html = response.get('body', b'')
    text = ''
    if row['status'] == 'ok':
        content_type = headers.get('content-type', '').lower()
        if content_type and 'html' not in content_type and not html.lstrip().lower().startswith((b'<!doctype html', b'<html')):
            row['status'] = 'non_html'
        else:
            soup = BeautifulSoup(html, 'lxml')
            text = extract_visible_text(soup)
            service_markers = ['rate limit exceeded', 'too many requests', 'wayback machine has not archived',
                               'checking your browser', 'just a moment...', 'verify you are human']
            service_failure = any(marker in text.lower() for marker in service_markers)
            js_notice=any(marker in text.lower() for marker in ['enable javascript','javascript is required'])
            if source == 'live' and client.config['render_fallback'] and (len(text) < 150 or service_failure or js_notice):
                aid = store.attempt(work_key, url + '#isolated-browser')
                rendered = render_live(response['url'], client.config)
                store.finish_attempt(aid, rendered['status'], {k:v for k,v in rendered.items() if k != 'body'})
                if rendered['status'] == 'ok' and len(rendered['body']) <= client.config['max_response_bytes']:
                    rendered_text = extract_visible_text(BeautifulSoup(rendered['body'], 'lxml'))
                    if len(rendered_text) > len(text):
                        html, text = rendered['body'], rendered_text
                        row.update(render_used=True, captured_at=rendered['fetched_at'], effective_url=rendered['url'])
                        row['html_path'], row['sha256'] = store.blob(html)
                else:
                    row['render_error'] = rendered.get('error', 'render_failed')
            row['identity_status'] = 'confirmed' if identity_match(candidate, text) or (
                inherited_identity and host(original_url(url)) == host(original_url(row['effective_url']))) else 'unresolved'
            row['status'] = 'usable' if text.strip() else 'no_content'
            if any(marker in text.lower() for marker in service_markers):
                row['status'] = 'request_error'
                row['error'] = 'service_or_browser_challenge'
            elif row['status'] == 'usable' and row['identity_status'] != 'confirmed':
                row['status'] = 'identity_unresolved'
            if source == 'wayback' and (not stamp or not event or capture_side(captured_at, event) != side):
                row['status'] = 'timestamp_mismatch'
            if text:
                text_rel = Path('text') / f"{hashlib.sha256(text.encode()).hexdigest()}.txt.gz"
                if not (store.root / text_rel).exists():
                    atomic(store.root / text_rel, gzip.compress(text.encode(), mtime=0))
                row['text_path'] = str(text_rel)
                row['n_char'] = len(text)
    if row['html_path']:
        row['compressed_bytes'] = (store.root / row['html_path']).stat().st_size
    store.page(row)
    return row, html


def available_captures(store, keys, event, side, url=None):
    from src.collection_2026_outputs import capture_side
    history = {}
    for key in keys:
        source = store.db.execute('SELECT candidate_id,status,payload FROM work WHERE key=?', (key,)).fetchone()
        if source is None:
            # Preserve the direct Store.capture interface for reviewed imports.
            history[key] = ''
            continue
        if source['status'] not in {'complete','empty'}:
            continue
        payload = json.loads(source['payload'])
        prefix, through = payload.get('prefix'), payload.get('through_date', '')
        history[key] = through
        if prefix:
            for previous in store.db.execute("SELECT key,payload FROM work WHERE kind='cdx_inventory' AND candidate_id=? AND status IN ('complete','empty')", (source['candidate_id'],)):
                if json.loads(previous['payload']).get('prefix') == prefix:
                    history[previous['key']] = max(history.get(previous['key'], ''), through)
    records = []
    for key, through in history.items():
        for r in store.db.execute("SELECT data FROM captures WHERE inventory_key=? OR json_extract(data,'$.latest_complete_inventory_key')=?", (key,key)):
            row = json.loads(r[0])
            if capture_omission_reason(row):
                continue
            if row.get('statuscode') not in {'200','301','302','303','307','308','-',''}:
                continue
            if through and row['timestamp'][:8] > through:
                continue
            if capture_side(timestamp_iso(row['timestamp']), event) != side:
                continue
            if url is not None and canonical(row['original']) != canonical(url):
                continue
            records.append(row)
    return sorted({(r['timestamp'],r['original']):r for r in records}.values(),
                  key=lambda r:r['timestamp'], reverse=side == 'pre')


def canonical(url):
    p = urlsplit(clean_url(url))
    return host(url) + (p.path.rstrip('/') or '/') + ('?' + p.query if p.query else '')


def crawl_snapshot(store, client, candidate, url, work_key, source, event=None, side='', inventory_keys=()):
    if source == 'wayback':
        from src.collection_2026_outputs import capture_side
        stamp = REPLAY.match(url).group(1)
        existing = store.db.execute("SELECT data FROM pages WHERE json_extract(data,'$.candidate_id')=? AND json_extract(data,'$.requested_timestamp')=?",
                                    (candidate['candidate_id'],stamp))
        for saved in existing:
            page = json.loads(saved['data'])
            if (page['page_type']=='homepage' and page['status']=='usable' and page.get('snapshot_complete') and
                    canonical(page['original_url'])==canonical(original_url(url)) and capture_side(page['captured_at'],event)==side):
                return {'usable':True,'snapshot_id':page['snapshot_id'],'pages':0,'reused':True,'retryable':False}
    # Each crawl is a new observation, including a retry after an interrupted live crawl.
    # The stable work key prevents repeated completed jobs; blobs still deduplicate bytes.
    snap_id = stable(VERSION, work_key, url, uuid.uuid4().hex)
    queue = [(url, 'homepage', 0)]
    seen, omissions, rows = set(), [], []
    homepage_identity = False
    while queue:
        page_url, page_type, depth = queue.pop(0)
        normalized = canonical(original_url(page_url))
        if normalized in seen:
            continue
        seen.add(normalized)
        if len(rows) >= client.config['max_pages_per_snapshot']:
            omissions.append({'url': original_url(page_url), 'reason': 'page_limit', 'depth': depth})
            continue
        alternatives = [page_url]
        if source == 'wayback' and depth > 0:
            stamp = REPLAY.match(page_url).group(1)
            alternatives += [f"https://web.archive.org/web/{c['timestamp']}id_/{c['original']}"
                             for c in sorted(available_captures(store, inventory_keys, event, side, original_url(page_url)),
                                             key=lambda c:abs((datetime.fromisoformat(timestamp_iso(c['timestamp']))-
                                                               datetime.fromisoformat(timestamp_iso(stamp))).total_seconds()))
                             if c['timestamp'] != stamp]
        for alternative in alternatives:
            if len(rows) >= client.config['max_pages_per_snapshot']:
                omissions.append({'url':original_url(alternative),'reason':'page_limit','depth':depth})
                break
            row, html = acquire_page(store, client, candidate, snap_id, alternative, page_type, depth, source,
                                     work_key, event, side, inherited_identity=homepage_identity)
            rows.append(row)
            if row['status'] == 'usable':
                break
        if depth == 0:
            homepage_identity = row['identity_status'] == 'confirmed' and row['status'] == 'usable'
        if row['status'] != 'usable':
            continue
        for link, child_type in page_links(html, row['effective_url'], source, client.config['max_depth'],parent_type=page_type):
            if canonical(link) in seen:
                continue
            if depth >= client.config['max_depth']:
                omissions.append({'url': link, 'reason': 'depth_limit', 'depth': depth+1})
                continue
            if source == 'wayback':
                captures = available_captures(store, inventory_keys, event, side, link)
                captures.sort(key=lambda x:abs((datetime.fromisoformat(timestamp_iso(x['timestamp'])) - datetime.fromisoformat(row['captured_at'])).total_seconds()))
                if not captures:
                    omissions.append({'url': link, 'reason': 'no_eligible_subpage_capture', 'depth': depth+1})
                    continue
                chosen = captures[0]
                link = f"https://web.archive.org/web/{chosen['timestamp']}id_/{chosen['original']}"
            queue.append((link, child_type, depth+1))
    # A failed attempt is resolved when another eligible capture of the same page succeeds.
    successful_urls = {canonical(r['original_url']) for r in rows if r['status']=='usable'}
    retryable = any(r['status']=='request_error' and canonical(r['original_url']) not in successful_urls for r in rows)
    for row in rows:
        row['snapshot_complete'] = not retryable
        store.page(row)
    write_json(store.root / 'snapshots' / f'{snap_id}.json',
               dict(snapshot_id=snap_id, work_key=work_key, page_ids=[r['page_id'] for r in rows], omissions=omissions))
    return dict(snapshot_id=snap_id, pages=len(rows), usable=bool(rows and rows[0]['status']=='usable'),
                omissions=len(omissions), retryable=retryable)


def collect_live(store, client, candidate, day):
    key = stable(VERSION, 'live', candidate['candidate_id'], day, candidate.get('website_url',''))
    store.add_work(key, 'live_snapshot', candidate['candidate_id'], {'day':day,'url':candidate.get('website_url','')})
    prior = store.db.execute('SELECT status FROM work WHERE key=?',(key,)).fetchone()
    if prior['status'] == 'complete':
        return
    url = clean_url(candidate.get('website_url',''))
    if not url:
        store.update(key, 'blocked', {'reason':'missing_url'})
        return
    store.start(key)
    alternatives=candidate.get('website_urls',[])
    if isinstance(alternatives,str):
        alternatives=json.loads(alternatives or '[]')
    urls=list(dict.fromkeys([url]+[clean_url(u) for u in alternatives if clean_url(u)]))
    results=[]
    for current in urls:
        result = crawl_snapshot(store,client,candidate,current,key,'live')
        results.append(result)
        if result['usable']:
            break
    retryable=results[-1]['retryable'] if results[-1]['usable'] else any(r['retryable'] for r in results)
    store.update(key, 'retryable' if retryable else 'complete',dict(results[-1],alternative_results=results))


def collect_archive(store, client, candidate, events, through_date):
    from src.collection_2026_calendar import events_for_candidate
    urls = candidate.get('website_urls', [])
    if isinstance(urls,str):
        try:
            urls=json.loads(urls)
        except ValueError:
            urls=[]
    urls = sorted({clean_url(u) for u in urls + [candidate.get('website_url','')] if isinstance(u,str)} - {''})
    if not urls:
        key=stable(VERSION,'cdx',candidate['candidate_id'],'missing_url',through_date)
        store.add_work(key,'cdx_inventory',candidate['candidate_id'],{})
        store.update(key,'blocked',{'reason':'missing_url'})
        return
    variants=set(urls)
    for url in urls:
        parts=urlsplit(url)
        alternate=parts.hostname.removeprefix('www.') if parts.hostname.startswith('www.') else 'www.'+parts.hostname
        variants.add(urlunsplit((parts.scheme,alternate,parts.path,parts.query,'')))
    keys=[inventory(store,client,candidate,url,through_date) for url in sorted(variants)]
    if any(store.db.execute('SELECT status FROM work WHERE key=?',(k,)).fetchone()[0] not in {'complete','empty'} for k in keys):
        return
    for event in events_for_candidate(candidate,events):
        if event.get('participation') in {'no','not_participating','not_participant','not_on_ballot','withdrawn','ineligible'} or event.get('verification_status') == 'unresolved':
            continue
        if event['date'].replace('-','') > through_date:
            # The pre window of a forthcoming event may already have captures.
            from datetime import date, timedelta
            if (date.fromisoformat(event['date'])-timedelta(days=30)).strftime('%Y%m%d') > through_date:
                continue
        for side in ('pre','post'):
            key=stable(VERSION,'archive',candidate['candidate_id'],event['event_id'],side,keys)
            store.add_work(key,'archive_snapshot',candidate['candidate_id'],{'inventory_keys':keys,'event':event},event['event_id'],side)
            prior=store.db.execute('SELECT status FROM work WHERE key=?',(key,)).fetchone()[0]
            if prior in {'complete','empty'}:
                continue
            store.start(key)
            captures=[capture for capture in available_captures(store,keys,event,side)
                      if any(canonical(capture['original'])==canonical(url) or
                             (urlsplit(url).path in {'','/'} and host(capture['original'])==host(url) and
                              classify_page_type(capture['original'])=='homepage') for url in urls)]
            captures=sorted({r['capture_id']:r for r in captures}.values(),key=lambda r:r['timestamp'],reverse=side=='pre')
            results=[]
            for capture in captures:
                replay=f"https://web.archive.org/web/{capture['timestamp']}id_/{capture['original']}"
                result=crawl_snapshot(store,client,candidate,replay,key,'wayback',event,side,keys)
                results.append(result)
                if result['usable']:
                    break
            retryable=any(r['retryable'] for r in results)
            success=any(r['usable'] for r in results)
            status='retryable' if retryable else ('complete' if results else 'empty')
            store.update(key,status,{'usable':success,'candidates_tried':len(results),'snapshots':results,
                                     'reason':'no_eligible_capture' if not captures else ''})
