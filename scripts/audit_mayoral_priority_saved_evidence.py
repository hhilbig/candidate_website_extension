"""Audit local historical validation records for the 107 priority URL rows."""
import csv
import hashlib
import json
import re
from collections import Counter
from pathlib import Path
from urllib.parse import urlsplit

ROOT = Path(__file__).resolve().parents[1]
QUEUE = Path('quality_reports/mayoral_review_reconciliation_2026-09-13/reconciliation/remaining_acquisition_review_queue.csv')
OUT = Path('quality_reports/mayoral_saved_evidence_followup_2026-09-13/priority')

def exact(url):
    p = urlsplit(url)
    return (p.scheme.lower(), p.netloc.lower(), p.path or '/', p.query)

def relation(a, b):
    if not a or not b:
        return 'missing_url'
    if exact(a) == exact(b):
        return 'exact_url'
    p, q = urlsplit(a), urlsplit(b)
    if (p.hostname or '').removeprefix('www.').lower() == (q.hostname or '').removeprefix('www.').lower():
        if (p.path or '/', p.query) == (q.path or '/', q.query):
            return 'syntactic_scheme_www_variant_not_verified_equivalent'
        return 'same_host_different_path_or_query'
    return 'different_host_same_candidate_year'

def truth(value):
    return str(value).lower() == 'true'

def effective_original(url):
    """Unwrap only recognized archive replay URLs; preserve original URL case."""
    p = urlsplit(url)
    if p.hostname in {'web.archive.org', 'wayback.archive-it.org'}:
        match = re.search(r'/\d{4,14}[a-z_]*\/(https?://.*)$', p.path + ('?' + p.query if p.query else ''))
        if match:
            return match.group(1)
    return url

def verify_manifest(root, manifest):
    for entry in [manifest['queue'], *manifest['sources']]:
        if hashlib.sha256((root / entry['path']).read_bytes()).hexdigest() != entry['sha256']:
            raise ValueError('Frozen input changed: ' + entry['path'])

def main():
    manifest_path = ROOT / OUT / 'source_manifest.json'
    manifest = json.loads(manifest_path.read_text())
    assert manifest['queue']['path'] == str(QUEUE)
    verify_manifest(ROOT, manifest)
    queue = list(csv.DictReader((ROOT / QUEUE).open()))
    targets = [r for r in queue if r['review_action'] == 'check_prior_acquisition_then_inventory_exact_url']
    assert len(targets) == 107 and len({r['candidate_url_id'] for r in targets}) == 107
    indexed = {}
    for row in targets:
        indexed.setdefault(row['candidate_year_id'], []).append(row)
    # Use the frozen source universe and reject any changed input.
    sources = manifest['sources']
    evidence = []
    for entry in sources:
        path = ROOT / entry['path']
        with path.open() as handle:
            reader = csv.DictReader(handle)
            fields = reader.fieldnames or []
            if 'candidate_year_id' not in fields or not ({'nearest_capture_timestamp', 'capture_effective_timestamp', 'archive_capture_count'} & set(fields)):
                continue
            for number, row in enumerate(reader, 2):
                for target in indexed.get(row['candidate_year_id'], []):
                    url = row.get('exact_normalized_url') or row.get('validated_campaign_url') or row.get('campaign_url') or row.get('url') or ''
                    match = relation(target['exact_normalized_url'], url)
                    metadata = bool(row.get('nearest_capture_timestamp') or row.get('capture_effective_timestamp'))
                    status = row.get('capture_fetch_status', '')
                    fetched = status.startswith('fetched') and bool(row.get('capture_content_sha256'))
                    effective = row.get('capture_effective_url', '')
                    original = effective_original(effective)
                    evidence.append({'candidate_url_id': target['candidate_url_id'], 'candidate_year_id': target['candidate_year_id'], 'source_path': str(path.relative_to(ROOT)), 'source_row': number, 'source_url': url, 'url_relation': match, 'effective_replay_url': effective, 'effective_original_url': original, 'effective_url_relation': relation(target['exact_normalized_url'], original), 'capture_metadata_present': metadata, 'saved_response_reported': fetched, 'campaign_support_reported': truth(row.get('historical_campaign_content_supported')) or truth(row.get('strict_coverage_eligible_result')), 'local_saved_bytes_verified': False, 'source_record': row})
    results = []
    for target in targets:
        matches = [e for e in evidence if e['candidate_url_id'] == target['candidate_url_id']]
        direct = [e for e in matches if e['url_relation'] == 'exact_url']
        capture = any(e['capture_metadata_present'] for e in direct)
        saved = any(e['saved_response_reported'] for e in direct)
        supported = any(e['campaign_support_reported'] for e in direct)
        effective_exact = any(e['saved_response_reported'] and e['effective_url_relation'] == 'exact_url' for e in direct)
        effective_variant = any(e['saved_response_reported'] and e['effective_url_relation'] == 'syntactic_scheme_www_variant_not_verified_equivalent' for e in direct)
        effective_changed = any(e['saved_response_reported'] and e['effective_url_relation'] in {'same_host_different_path_or_query', 'different_host_same_candidate_year'} for e in direct)
        state = ('inspect_reported_campaign_capture' if supported else 'locate_reported_saved_bytes' if saved else 'reuse_capture_metadata_before_new_inventory' if capture else 'review_prior_exact_url_attempts' if direct else 'no_exact_url_validation_in_scanned_tables')
        malformed = bool(urlsplit(target['exact_normalized_url']).username)
        if malformed:
            state = 'review_url_with_userinfo_before_any_request'
        results.append({k: target[k] for k in ['candidate_url_id','candidate_year_id','full_name','geo_name','year','exact_normalized_url']} | {'exact_url_prior_rows': len(direct), 'same_candidate_other_url_rows': len(matches)-len(direct), 'exact_capture_metadata_present': capture, 'exact_saved_response_reported': saved, 'exact_campaign_support_reported': supported, 'effective_exact_response_reported': effective_exact, 'effective_scheme_www_variant_response_reported': effective_variant, 'effective_changed_target_response_reported': effective_changed, 'local_saved_bytes_verified': False, 'url_has_userinfo': malformed, 'next_action': state})
    out = ROOT / OUT
    out.mkdir(parents=True, exist_ok=True)
    with (out / 'priority_audit.csv').open('w') as handle:
        writer = csv.DictWriter(handle, fieldnames=list(results[0]))
        writer.writeheader()
        writer.writerows(results)
    (out / 'prior_evidence.json').write_text(json.dumps(evidence, indent=2) + '\n')
    summary = {'target_rows': len(results), 'candidate_years': len(indexed), 'input_queue': str(QUEUE), 'input_sha256': hashlib.sha256((ROOT / QUEUE).read_bytes()).hexdigest(), 'sources': sources, 'script_sha256': hashlib.sha256(Path(__file__).read_bytes()).hexdigest(), 'counts': {k: sum(bool(r[k]) for r in results) for k in ['exact_capture_metadata_present', 'exact_saved_response_reported', 'exact_campaign_support_reported', 'local_saved_bytes_verified', 'url_has_userinfo']}, 'next_actions': dict(Counter(r['next_action'] for r in results)), 'limitations': ['Historical rows are preserved, not independent claims or current approvals.', 'Capture timestamps may fall outside the election window.', 'Fetch reports and content hashes do not establish that response bytes remain available.', 'Other hosts and paths do not establish exact URL capture coverage.', 'No archive absence claim follows from this local audit.', 'No remote or archive requests occurred.']}
    summary['field_definitions'] = {'exact_capture_metadata_present': 'A prior source record matches the requested exact URL and reports capture metadata; effective target may differ.', 'exact_saved_response_reported': 'A prior source record matches the requested exact URL and reports fetched content with a hash; effective target may differ.', 'exact_campaign_support_reported': 'A matching requested-URL source record reports campaign support; this is not renewed adjudication.'}
    summary['effective_response_counts'] = {k: sum(r[k] for r in results) for k in ['effective_exact_response_reported', 'effective_scheme_www_variant_response_reported', 'effective_changed_target_response_reported']}
    summary['source_manifest'] = str(manifest_path.relative_to(ROOT))
    summary['source_manifest_sha256'] = hashlib.sha256(manifest_path.read_bytes()).hexdigest()
    (out / 'summary.json').write_text(json.dumps(summary, indent=2) + '\n')
    print(json.dumps(summary['counts']))
    print(json.dumps(summary['next_actions']))

if __name__ == '__main__':
    main()
