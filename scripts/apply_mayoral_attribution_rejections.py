"""Exclude evidence-bound wrong-candidate attributions; retain reference rows."""
import argparse
import csv
import hashlib
import json
from pathlib import Path

VERSION = 'mayoral-ai-attribution-exclusions-v1'


def indexed(rows):
    result = {r['candidate_url_id']: r for r in rows}
    if len(result) != len(rows) or '' in result:
        raise ValueError('Duplicate or empty candidate URL key')
    return result


def apply_rejections(main, queue, packets, queue_hash):
    source, evidence = indexed(main), indexed(queue)
    rejected = {}
    seen = set()
    for filename, packet in packets:
        if packet['input_sha256'] != queue_hash:
            raise ValueError('Review queue hash mismatch')
        for review in packet['records']:
            key = review['candidate_url_id']
            if key in seen or key not in source or key not in evidence:
                raise ValueError('Duplicate or unknown review key')
            seen.add(key)
            row, saved = source[key], evidence[key]
            if (review['candidate_year_id'] != row['candidate_year_id']
                    or review['candidate_year_id'] != saved['candidate_year_id']
                    or not row.get('exact_normalized_url')
                    or row['exact_normalized_url'] != saved.get('exact_normalized_url')
                    or review['capture_sha256'] != saved['evidence_sha256']
                    or review['capture_timestamp'] != saved['effective_timestamp']
                    or review['hash_verified'] is not True):
                raise ValueError('Review evidence mismatch')
            if review['decision'] != 'reject_candidate_attribution':
                continue
            if not review.get('reason') or not packet.get('reviewer'):
                raise ValueError('Attribution rejection lacks reason or reviewer')
            if any(str(row.get(f, '')).lower() == 'true' for f in
                   ['human_accepted', 'ai_accepted_website']):
                raise ValueError('Rejection conflicts with prior acceptance')
            additions = {
                'attribution_exclusion_version': VERSION,
                'attribution_exclusion_reason': review['reason'],
                'attribution_exclusion_review_file': filename,
                'attribution_exclusion_reviewer': packet['reviewer'],
                'attribution_exclusion_model': packet.get('model', 'not recorded'),
                'attribution_exclusion_capture_sha256': review['capture_sha256'],
                'attribution_exclusion_capture_timestamp': review['capture_timestamp'],
                'attribution_exclusion_evidence_path': saved['evidence_path'],
            }
            if set(row) & set(additions):
                raise ValueError('Refusing to overwrite exclusion columns')
            rejected[key] = {**row, **additions}
    return ([r.copy() for r in main if r['candidate_url_id'] not in rejected],
            list(rejected.values()))


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--main', type=Path, required=True)
    p.add_argument('--queue', type=Path, required=True)
    p.add_argument('--reviews', type=Path, nargs='+', required=True)
    p.add_argument('--output-dir', type=Path, required=True)
    args = p.parse_args()
    paths = [args.main, args.queue, *args.reviews]
    hashes = {str(x): hashlib.sha256(x.read_bytes()).hexdigest() for x in paths}
    with args.main.open() as f:
        rows = list(csv.DictReader(f))
    queue = json.loads(args.queue.read_text())
    packets = [(str(x), json.loads(x.read_text())) for x in args.reviews]
    kept, rejected = apply_rejections(rows, queue, packets, hashes[str(args.queue)])
    if not rows or not rejected:
        raise ValueError('Expected nonempty inputs and explicit rejections')
    args.output_dir.mkdir(parents=True, exist_ok=False)
    for name, records in [('candidate_urls.csv', kept), ('rejected_reference_urls.csv', rejected)]:
        fields = list(dict.fromkeys([*rows[0], *(k for r in records for k in r)]))
        with (args.output_dir / name).open('x', newline='') as f:
            w = csv.DictWriter(f, fieldnames=fields)
            w.writeheader()
            w.writerows(records)
    summary = dict(version=VERSION, input_hashes=hashes,
                   url_rows_before=len(rows), url_rows_after=len(kept),
                   excluded_url_rows=len(rejected),
                   candidate_years_before=len({r['candidate_year_id'] for r in rows}),
                   candidate_years_after=len({r['candidate_year_id'] for r in kept}),
                   excluded_keys=sorted(r['candidate_url_id'] for r in rejected),
                   interpretation='Explicit wrong-candidate rejections only. Remaining rows are discovery leads, not approved websites. Candidate-year totals include inactive rows.',
                   output_hashes={x.name: hashlib.sha256(x.read_bytes()).hexdigest()
                                  for x in args.output_dir.glob('*.csv')})
    (args.output_dir / 'summary.json').write_text(json.dumps(summary, indent=2) + '\n')
    print(json.dumps(summary, indent=2))


if __name__ == '__main__':
    main()
