"""Audit accepted reviews against saved captures without archive requests."""
import argparse
import csv
import hashlib
import inspect
import json
from pathlib import Path
import shlex
import subprocess
from collections import Counter
from datetime import datetime, timezone


def inspect_capture(record):
    """Read saved bytes and extract static text without executing HTML."""
    import gzip
    import hashlib
    import re
    from pathlib import Path
    from bs4 import BeautifulSoup

    result = dict(record)
    result.update(hash_verified=False, saved_text_available=False,
                  persisted_extracted_text_available=None,
                  persisted_extracted_text_status='not_audited',
                  visible_text_characters=0, visible_text_words=0,
                  stored_bytes=0, decompressed_bytes=0, status='needs_review')
    path = Path(record['capture_path'])
    try:
        stored = path.read_bytes()
        raw = gzip.decompress(stored) if path.suffix == '.gz' else stored
        actual_hash = hashlib.sha256(raw).hexdigest()
        result.update(stored_bytes=len(stored), decompressed_bytes=len(raw),
                      actual_capture_sha256=actual_hash,
                      hash_verified=actual_hash == record['capture_sha256'])
        soup = BeautifulSoup(raw, 'html.parser')
        for node in soup.select('script,style,noscript,template,svg,head,iframe,object,[hidden],[aria-hidden="true"]'):
            node.decompose()
        for node in soup.find_all(style=True):
            if node.attrs is not None and re.search(r'(display\s*:\s*none|visibility\s*:\s*hidden)', node.get('style', ''), re.I):
                node.decompose()
        visible = ' '.join(soup.stripped_strings)
        result.update(visible_text_characters=len(visible), visible_text_words=len(visible.split()),
                      visible_text_excerpt=visible[:700],
                      visible_text_sha256=hashlib.sha256(visible.encode()).hexdigest())
        result['saved_text_available'] = bool(visible) and result['hash_verified']
        if result['saved_text_available']:
            result['status'] = 'usable_saved_campaign_text'
        else:
            result['reason'] = 'hash_mismatch' if not result['hash_verified'] else 'empty_static_text'
    except FileNotFoundError:
        result.update(status='missing_file', reason='review_capture_path_missing')
    except (OSError, EOFError, ValueError) as error:
        result['reason'] = f'{type(error).__name__}: {error}'
    return result


def select_records(rows, packets):
    by_key = {row['candidate_url_id']: row for row in rows}
    if len(by_key) != len(rows):
        raise ValueError('Duplicate validation keys')
    selected = []
    seen = set()
    for packet in packets:
        for review in packet['records']:
            key = review['candidate_url_id']
            if key in seen:
                raise ValueError('Duplicate review keys')
            seen.add(key)
            if review['decision'] != 'accept':
                continue
            source = by_key[key]
            for review_field, source_field in [
                ('capture_sha256', 'capture_content_sha256'),
                ('capture_effective_timestamp', 'capture_effective_timestamp'),
                ('candidate_year_id', 'candidate_year_id'),
            ]:
                if review[review_field] != source[source_field]:
                    raise ValueError(f'Review evidence differs from validation: {key} {review_field}')
            if source['ai_review_decision'] != 'accept':
                raise ValueError('Validation decision differs from review')
            selected.append(dict(candidate_url_id=key,
                candidate_year_id=review['candidate_year_id'], name=review['name'],
                capture_path=review['capture_path'], capture_sha256=review['capture_sha256'],
                capture_effective_timestamp=source['capture_effective_timestamp'],
                capture_effective_url=source['capture_effective_url'],
                campaign_relevance_basis='existing_ai_acceptance_of_same_capture',
                campaign_review_reason=review['rationale']))
    expected = {row['candidate_url_id'] for row in rows
                if row.get('ai_reviewed', '').lower() == 'true'
                and row.get('ai_review_decision') == 'accept'}
    if {record['candidate_url_id'] for record in selected} != expected:
        raise ValueError('Accepted review keys differ from validation accepted keys')
    return sorted(selected, key=lambda record: record['candidate_url_id'])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--validation', type=Path, required=True)
    parser.add_argument('--reviews', type=Path, nargs='+', required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--host', default='root@droplet')
    parser.add_argument('--remote-python', default='/root/candidate_website_extension/.venv/bin/python')
    args = parser.parse_args()
    with args.validation.open() as handle:
        rows = list(csv.DictReader(handle))
    packets = [json.loads(path.read_text()) for path in args.reviews]
    records = select_records(rows, packets)
    source = ('import json\n' + inspect.getsource(inspect_capture)
              + '\nrecords = json.loads(' + repr(json.dumps(records)) + ')\n'
              + 'print(json.dumps([inspect_capture(record) for record in records]))\n')
    run = subprocess.run(['ssh', '-o', 'BatchMode=yes', '-o', 'ConnectTimeout=15',
                          args.host, shlex.quote(args.remote_python) + ' -'],
                         input=source, capture_output=True, text=True, timeout=180)
    if run.returncode:
        raise RuntimeError(f'Remote audit failed: {run.stderr.strip()}')
    results = json.loads(run.stdout)
    keys = [result['candidate_url_id'] for result in results]
    if len(keys) != len(set(keys)) or set(keys) != {r['candidate_url_id'] for r in records}:
        raise ValueError('Remote result keys differ from selected keys')
    unique = {result['capture_path']: result for result in results}
    output = dict(audited_at_utc=datetime.now(timezone.utc).isoformat(), host=args.host,
        scope='One reviewed saved campaign capture per accepted candidate URL; full snapshot inventory is not audited.',
        text_method='BeautifulSoup static text; excludes scripts, styles, templates, head, embedded objects, and explicit hidden elements. CSS is not rendered.',
        text_availability_definition='Verified raw HTML can regenerate nonempty static text. Separate extracted-text files are not audited.',
        input_sha256={str(path): hashlib.sha256(path.read_bytes()).hexdigest()
                      for path in [args.validation, *args.reviews]},
        expected_urls=len(records), observed_urls=len(results),
        statuses=dict(Counter(result['status'] for result in results)),
        unique_capture_files=len(unique), unique_stored_bytes=sum(r['stored_bytes'] for r in unique.values()),
        unique_decompressed_bytes=sum(r['decompressed_bytes'] for r in unique.values()),
        archive_requests=0, raw_files_copied=0,
        records={result['candidate_url_id']: result for result in results})
    with args.output.open('x') as handle:
        json.dump(output, handle, indent=2)
        handle.write('\n')
    print(json.dumps({key: value for key, value in output.items() if key != 'records'}, indent=2))


if __name__ == '__main__':
    main()
