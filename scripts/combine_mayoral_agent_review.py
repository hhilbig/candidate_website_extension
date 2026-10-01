"""Combine advisory website reviews without changing research approvals."""
import argparse
import csv
import hashlib
import json
from collections import Counter
from pathlib import Path


def combine(validation, reviews):
    expected = {r['candidate_url_id']: r for r in validation
                if r['strict_coverage_eligible_result'].lower() == 'true'}
    keys = [r['candidate_url_id'] for r in reviews]
    if len(keys) != len(set(keys)) or set(keys) != set(expected):
        raise ValueError('Review keys must match cleared proposal keys exactly')
    for review in reviews:
        source = expected[review['candidate_url_id']]
        if review['candidate_year_id'] != source['candidate_year_id']:
            raise ValueError('Candidate-year mismatch')
        if review['recommendation'] not in {'accept', 'review', 'reject'}:
            raise ValueError('Unknown recommendation')
    return {
        'review_type': 'advisory_agent_review',
        'human_approvals_added': 0,
        'input_proposals': len(validation),
        'reviewed_cleared_proposals': len(reviews),
        'recommendations': dict(Counter(r['recommendation'] for r in reviews)),
        'unique_candidate_years': len({r['candidate_year_id'] for r in reviews}),
        'records': sorted(reviews, key=lambda r: r['candidate_url_id']),
    }


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--validation', type=Path, required=True)
    parser.add_argument('--reviews', type=Path, nargs='+', required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    rows = list(csv.DictReader(args.validation.open()))
    reviews = []
    for path in args.reviews:
        data = json.loads(path.read_text())
        reviews.extend(data if isinstance(data, list) else data['records'])
    result = combine(rows, reviews)
    result['input_sha256'] = hashlib.sha256(args.validation.read_bytes()).hexdigest()
    result['review_file_hashes'] = {str(p): hashlib.sha256(p.read_bytes()).hexdigest() for p in args.reviews}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open('x') as handle:
        json.dump(result, handle, indent=2)
        handle.write('\n')
