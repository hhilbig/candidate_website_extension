#!/usr/bin/env python3
"""Locate historical response bytes without archive requests or raw file copies."""
import collections
import csv
import gzip
import hashlib
import json
from pathlib import Path
import re
import subprocess
from urllib.parse import urlsplit, urlunsplit

INPUT = Path('quality_reports/mayoral_saved_evidence_followup_2026-09-13/priority/prior_evidence.json')
OUT = Path('quality_reports/mayoral_priority_files_2026-09-14')


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def exact(url):
    p = urlsplit(url)
    return urlunsplit((p.scheme.lower(), p.netloc.lower(), p.path or '/', p.query, ''))


def replay(url):
    m = re.search(r'/(\d{14})(?:id_)?/(https?://.*)', url)
    return (m.group(1), exact(m.group(2))) if m else ('', '')


def byte_hashes(raw):
    result = {'stored_response_bytes': sha(raw)}
    # Archive-It validators hash response.text encoded as UTF-8.
    for encoding in ('utf-8', 'iso-8859-1', 'windows-1252'):
        result['response_text_' + encoding + '_to_utf8'] = sha(raw.decode(encoding, errors='replace').encode('utf-8'))
    return result


def scan(evidence, remote=False):
    expected = {e['source_record']['capture_content_sha256'] for e in evidence}
    urls = {exact(e['source_url']) for e in evidence}
    candidate_ids = {e['candidate_year_id'].split('__')[0] for e in evidence}
    hosts = {(urlsplit(u).hostname or '').removeprefix('www.') for u in urls}
    inspections, matches, checked, errors = [], [], {}, []

    def check(path, provenance):
        path = Path(path)
        if not path.is_file():
            errors.append({'path': str(path), 'error': 'referenced_file_missing'})
            return
        if str(path) not in checked:
            try:
                packed = path.read_bytes()
                raw = gzip.decompress(packed) if path.suffix == '.gz' else packed
                checked[str(path)] = (byte_hashes(raw), len(raw), len(packed))
            except (OSError, EOFError) as error:
                errors.append({'path': str(path), 'error': str(error)})
                return
        hashes, size, stored = checked[str(path)]
        hit = {basis: h for basis, h in hashes.items() if h in expected}
        if hit:
            matches.append({'path': str(path), 'hash_matches': hit, 'raw_bytes': size,
                            'stored_bytes': stored, 'raw_sha256': hashes['stored_response_bytes'],
                            'provenance': provenance})

    if remote:
        base = Path('/root/candidate_website_extension/data/municipal_archive_acquisition')
        names = ['covered_151_20260826', 'openai_residual_100_20260826', 'openai_residual_expansion_357_20260826', 'full_discovery_740_20260826']
        roots = [base/n/'runs/production' for n in names]
        roots.append(Path('/mnt/mayoral_wayback_full_snapshots/runs/production_all_inventory'))
        for root in roots:
            for filename in ('fetch_attempts.csv', 'fetch_attempts.journal.csv'):
                ledger = root/filename
                if not ledger.exists():
                    inspections.append({'path': str(ledger), 'exists': False})
                    continue
                total = selected = 0
                with ledger.open(newline='') as handle:
                    for row in csv.DictReader(handle):
                        total += 1
                        same_candidate = row.get('candidate_year_id', '').split('__')[0] in candidate_ids
                        same_host = (urlsplit(row.get('original_url', '')).hostname or '').removeprefix('www.') in hosts
                        if row.get('raw_html_sha256') in expected or (same_candidate and same_host):
                            selected += 1
                            if row.get('raw_html_path'):
                                check(root/row['raw_html_path'], {k: row.get(k, '') for k in ('candidate_year_id','original_url','timestamp','effective_replay_url','effective_timestamp','raw_html_sha256','fetch_status')})
                inspections.append({'path': str(ledger), 'exists': True, 'rows': total, 'selected_rows': selected, 'sha256': sha(ledger.read_bytes())})
        owner = Path('/mnt/mayoral_wayback_full_snapshots/ownership_validation_20260910')
        for ledger in owner.rglob('requests.jsonl'):
            total = 0
            for line in ledger.read_text().splitlines():
                row = json.loads(line)
                total += 1
                if row.get('evidence_file'):
                    check(ledger.parent/row['evidence_file'], row)
            inspections.append({'path': str(ledger), 'exists': True, 'rows': total, 'sha256': sha(ledger.read_bytes())})
    else:
        # Search ignored files too, across both discovery and saved report trees.
        for root in (Path('data/municipal_discovery'), Path('data/municipal_archive_acquisition'), Path('quality_reports')):
            files = 0
            for path in root.rglob('*'):
                if path.is_file() and (path.suffix in {'.html', '.htm'} or path.name.endswith(('.html.gz', '.htm.gz'))):
                    files += 1
                    check(path, {'source': 'local_html_inventory'})
            inspections.append({'path': str(root), 'exists': root.exists(), 'html_files': files})
    return {'inspected_stores': inspections, 'matching_files': matches, 'files_hashed': len(checked), 'errors': errors}


def main():
    evidence = [e for e in json.loads(INPUT.read_text()) if e['url_relation'] == 'exact_url' and e['saved_response_reported']]
    assert len(evidence) == 44
    grouped = collections.defaultdict(list)
    for row in evidence:
        grouped[row['candidate_url_id']].append(row)
    assert len(grouped) == 19
    source = Path(__file__).read_text().split('\ndef main():')[0]
    result = subprocess.run(['ssh','-o','BatchMode=yes','-o','ConnectTimeout=15','root@droplet','python3','-'], input=source+'\nprint(json.dumps(scan('+repr(evidence)+', remote=True)))\n', text=True, capture_output=True, timeout=300)
    if result.returncode:
        raise RuntimeError(result.stderr)
    remote = json.loads(result.stdout)
    local = scan(evidence)
    matches = remote['matching_files'] + local['matching_files']
    audit = []
    for key, rows in sorted(grouped.items()):
        hashes = {e['source_record']['capture_content_sha256'] for e in rows}
        hits = [m for m in matches if hashes.intersection(m['hash_matches'].values())]
        hit_paths = {m['path']: m for m in hits}
        matched_years = sorted({m['provenance'].get('candidate_year_id', '') for m in hits})
        audit.append({'candidate_url_id': key, 'candidate_year_id': rows[0]['candidate_year_id'], 'exact_url': exact(rows[0]['source_url']), 'status': 'verified_saved_bytes' if hits else 'no_matching_bytes_in_inspected_stores', 'prior_evidence_rows': len(rows), 'prior_hashes_json': json.dumps(sorted(hashes)), 'prior_capture_dates_json': json.dumps(sorted({str(e['source_record'].get('capture_effective_timestamp') or e['source_record'].get('nearest_capture_timestamp')) for e in rows})), 'matching_files': len(hit_paths), 'matching_corpus_candidate_years_json': json.dumps(matched_years), 'same_candidate_year_match': rows[0]['candidate_year_id'] in matched_years, 'matches_json': json.dumps(list(hit_paths.values()), sort_keys=True)})
    OUT.mkdir(parents=True, exist_ok=True)
    with (OUT/'audit.csv').open('w', newline='') as handle:
        writer = csv.DictWriter(handle, fieldnames=list(audit[0]))
        writer.writeheader()
        writer.writerows(audit)
    summary = {'input': str(INPUT), 'input_sha256': sha(INPUT.read_bytes()), 'script_sha256': sha(Path(__file__).read_bytes()), 'input_total_rows': 271, 'selected_evidence_rows': len(evidence), 'expected_url_keys': 19, 'observed_url_keys': len(audit), 'missing_keys': sorted(set(grouped)-{r['candidate_url_id'] for r in audit}), 'statuses': dict(collections.Counter(r['status'] for r in audit)), 'local': local, 'remote': remote, 'limitations': ['A matching hash verifies persisted content only. It does not establish campaign ownership or an eligible capture date.', 'The negative status applies only to the named inspected stores. It does not establish archive absence.', 'Archive-It historical hashes use decoded response text encoded as UTF-8. The audit compares raw bytes and three explicit text decodings.', 'Local HTML files and five remote fetch ledgers were searched. The remote ownership request journal supplied a separate response store.', 'No archive requests, remote writes, or raw response copies occurred.']}
    summary['same_candidate_year_url_matches'] = sum(r['same_candidate_year_match'] for r in audit)
    summary['capture_date_caveat'] = 'The sole byte match links the Bill Saffo 2011 lead to a 2019-10-18 capture stored under 38408__2019. It does not establish 2011 campaign evidence.'
    (OUT/'summary.json').write_text(json.dumps(summary, indent=2)+'\n')
    assert len(list(csv.DictReader((OUT/'audit.csv').open()))) == 19
    print(json.dumps({k: summary[k] for k in ('statuses','expected_url_keys','observed_url_keys','missing_keys')}))


if __name__ == '__main__':
    main()
