"""Attach model review decisions without changing existing validation fields."""
import argparse
import csv
import hashlib
import json
from collections import Counter
from pathlib import Path


def attach(rows, packets, input_hash):
    keys = [r['candidate_url_id'] for r in rows]
    if len(keys) != len(set(keys)):
        raise ValueError('Duplicate input keys')
    expected = {r['candidate_url_id']: r for r in rows
                if r['strict_coverage_eligible_result'].lower() == 'true'}
    reviews = {}
    for packet in packets:
        if packet['model'] != 'gpt-5.6-sol' or packet['review_type'] != 'ai_review':
            raise ValueError('Unexpected model or review type')
        if packet['input_sha256'] != input_hash:
            raise ValueError('Input hash mismatch')
        for review in packet['records']:
            key = review['candidate_url_id']
            if key in reviews or key not in expected:
                raise ValueError('Duplicate or unexpected review key')
            source = expected[key]
            for field, source_field in [('candidate_year_id', 'candidate_year_id'),
                                        ('url', 'raw_url'),
                                        ('capture_sha256', 'capture_content_sha256'),
                                        ('capture_effective_timestamp', 'capture_effective_timestamp')]:
                if review[field] != source[source_field]:
                    raise ValueError(f'Review mismatch: {field}')
            if review['decision'] not in {'accept', 'reject', 'unsure'}:
                raise ValueError('Invalid decision')
            if review['hash_verified'] is not True or not review['rationale'].strip():
                raise ValueError('Missing verified evidence or rationale')
            reviews[key] = review
    if set(reviews) != set(expected):
        raise ValueError('Incomplete review')
    result = []
    for source in rows:
        review = reviews.get(source['candidate_url_id'])
        additions = dict(ai_reviewed=str(review is not None),
                         ai_review_model='gpt-5.6-sol' if review else '',
                         ai_review_date='2026-09-14' if review else '',
                         ai_review_decision=review['decision'] if review else '',
                         ai_review_reason=review['rationale'] if review else '')
        if set(source) & set(additions):
            raise ValueError('AI review columns already exist')
        result.append({**source, **additions})
    return result, dict(Counter(r['decision'] for r in reviews.values()))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--validation', type=Path, required=True)
    parser.add_argument('--reviews', nargs='+', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    source_hash = hashlib.sha256(args.validation.read_bytes()).hexdigest()
    with args.validation.open() as handle:
        source = list(csv.DictReader(handle))
    packets = [json.loads(p.read_text()) for p in args.reviews]
    rows, counts = attach(source, packets, source_hash)
    args.output.mkdir(parents=True, exist_ok=True)
    with (args.output / 'validation.csv').open('x', newline='') as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    summary = dict(input_sha256=source_hash, rows=len(rows), decisions=counts,
                   ai_reviewed=sum(counts.values()), human_approvals_added=0,
                   model='gpt-5.6-sol', review_type='ai_review',
                   review_hashes={str(p): hashlib.sha256(p.read_bytes()).hexdigest()
                                  for p in args.reviews},
                   output_sha256=hashlib.sha256((args.output / 'validation.csv').read_bytes()).hexdigest())
    with (args.output / 'summary.json').open('x') as handle:
        json.dump(summary, handle, indent=2)
        handle.write('\n')
    print(json.dumps(summary, indent=2))


if __name__ == '__main__':
    main()
