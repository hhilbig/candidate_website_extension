"""Read-only inspection of saved supplemental mayoral captures; no network calls."""
import argparse
import csv
import datetime as dt
import gzip
import hashlib
import json
import re
from pathlib import Path
from urllib.parse import urlsplit

from bs4 import BeautifulSoup


def normalized(url):
    parsed = urlsplit(url)
    host = (parsed.hostname or '').lower().removeprefix('www.')
    port = parsed.port
    if port and not ((parsed.scheme == 'http' and port == 80) or (parsed.scheme == 'https' and port == 443)):
        host += ':' + str(port)
    return host, parsed.path or '/', parsed.query


def sha(path):
    digest = hashlib.sha256()
    with path.open('rb') as handle:
        for block in iter(lambda: handle.read(1048576), b''):
            digest.update(block)
    return digest.hexdigest()


def inspect(validation, corpus):
    with validation.open() as handle:
        leads = [r for r in csv.DictReader(handle) if r['final_validation_status'] == 'saved_capture_content_confirmed_reuse_pending']
    targets = {normalized(r['exact_normalized_url']) for r in leads}
    ledger = corpus / 'fetch_attempts.csv'
    current = {}
    with ledger.open() as handle:
        for row in csv.DictReader(handle):
            if normalized(row['original_url']) in targets:
                current[row['capture_id']] = row
    result = []
    for lead in leads:
        reference = dt.date.fromisoformat(lead['election_reference_date'])
        lower, upper = reference.replace(year=reference.year - 2), reference.replace(year=reference.year + 2)
        candidates = []
        for row in current.values():
            timestamp = row['effective_timestamp'] or row['timestamp']
            if row['fetch_status'] != 'fetched' or normalized(row['original_url']) != normalized(lead['exact_normalized_url']) or timestamp == lead['supplement_effective_timestamp']:
                continue
            date = dt.datetime.strptime(timestamp, '%Y%m%d%H%M%S').date()
            if lower <= date <= upper:
                candidates.append((timestamp, row['capture_id'], date, row))
        before = sorted(x for x in candidates if x[2] < reference)
        after = sorted(x for x in candidates if x[2] > reference)
        selected = ([('before', before[0])] if before else []) + ([('after', after[-1])] if after else [])
        record = {k: lead[k] for k in ['candidate_url_id', 'candidate_year_id', 'full_name', 'geo_name', 'exact_normalized_url', 'election_reference_date', 'supplement_capture_id', 'supplement_effective_timestamp']}
        record.update(saved_matching_before_count=len(before), saved_matching_after_count=len(after), captures=[])
        for side, item in selected:
            row = item[3]
            path = corpus / row['raw_html_path']
            assert path.resolve().is_relative_to(corpus.resolve())
            with gzip.open(path, 'rb') as handle:
                raw = handle.read()
            digest = hashlib.sha256(raw).hexdigest()
            assert digest == row['raw_html_sha256'], row['capture_id']
            soup = BeautifulSoup(raw, 'html.parser')
            title = soup.title.get_text(' ', strip=True) if soup.title else ''
            for node in soup(['script', 'style', 'noscript', 'svg']):
                node.decompose()
            visible = re.sub(r'\s+', ' ', soup.get_text(' ', strip=True))
            evidence = {k: row[k] for k in ['capture_id', 'original_url', 'requested_replay_url', 'effective_replay_url', 'timestamp', 'effective_timestamp', 'raw_html_path', 'raw_html_sha256', 'raw_html_bytes', 'attempted_at_utc']}
            evidence.update(side=side, hash_verified=True, title=title, visible_characters=len(visible), excerpt=visible[:5000])
            record['captures'].append(evidence)
        result.append(record)
    return {'selection_rule': 'Earliest saved successful effective capture before election reference and latest after, within plus/minus 24 calendar months; exclude main effective timestamp; preserve path and query; missing saved side does not prove archive absence.', 'validation_sha256': sha(validation), 'ledger_sha256': sha(ledger), 'script_sha256': sha(Path(__file__)), 'records': result}


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('validation', type=Path)
    parser.add_argument('corpus', type=Path)
    args = parser.parse_args()
    print(json.dumps(inspect(args.validation, args.corpus), ensure_ascii=False, indent=2))
