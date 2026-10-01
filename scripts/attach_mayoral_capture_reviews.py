"""Attach separate AI capture decisions without changing existing validation fields."""
import argparse
import csv
import hashlib
import json
from collections import Counter
from pathlib import Path


FIELDS = [
    'ai_capture_reviewed', 'ai_capture_review_model', 'ai_capture_review_decision',
    'ai_capture_review_reason', 'ai_capture_review_evidence_path',
    'ai_capture_review_evidence_sha256', 'ai_capture_review_timestamp',
    'ai_capture_review_remaining_flags', 'ai_capture_review_record_file',
]


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def unique(rows):
    keyed = {r['candidate_url_id']: r for r in rows}
    if len(keyed) != len(rows) or '' in keyed:
        raise ValueError('Duplicate or empty candidate URL key')
    return keyed


def attach(rows, packets, input_sha256):
    source = unique(rows)
    if any(set(row) & set(FIELDS) for row in rows):
        raise ValueError('Refusing to overwrite capture review fields')
    selected = {k for k, r in source.items()
                if r['historical_campaign_content_supported'].lower() == 'true'
                and r['ai_reviewed'].lower() != 'true'}
    if len(rows) != 120 or len(selected) != 30:
        raise ValueError('Expected 120 rows and 30 selected keys')
    reviews = []
    for filename, packet in packets:
        if packet['input_sha256'] != input_sha256:
            raise ValueError('Source input hash mismatch')
        if packet['model'] != 'gpt-5.6-sol':
            raise ValueError('Invalid model')
        reviews.extend({**r, 'record_file': str(filename), 'model': packet['model']}
                       for r in packet['reviews'])
    checked = unique(reviews)
    if set(checked) != selected:
        raise ValueError('Missing or unexpected selected keys')
    for key, review in checked.items():
        row = source[key]
        if any(review[f] != row[f] for f in ('candidate_year_id', 'candidate_contest_id')):
            raise ValueError('Candidate-year or contest mismatch')
        pair = (review['evidence_sha256'], review['evidence_timestamp'])
        pairs = {(row['capture_content_sha256'], row['capture_effective_timestamp']),
                 (row['supplement_raw_html_sha256'], row['supplement_effective_timestamp'])}
        if not all(pair) or pair not in pairs:
            raise ValueError('Evidence hash and timestamp pair mismatch')
        verification = [review[f] for f in ('hash_verified', 'decoded_sha256_verified') if f in review]
        if not verification or any(value is not True for value in verification):
            raise ValueError('Hash verification must be true')
        if ('observed_decoded_sha256' in review
                and review['observed_decoded_sha256'] != review['evidence_sha256']):
            raise ValueError('Observed evidence hash mismatch')
        flags = review['remaining_flags']
        if (not isinstance(flags, list) or not all(isinstance(f, str) for f in flags)
                or row['flag_resolution_status'] not in flags):
            raise ValueError('Exact flag resolution status was lost')
        if review['decision'] not in {'support_capture', 'reject_capture', 'uncertain'}:
            raise ValueError('Invalid decision')
        if not review['rationale'] or not review['evidence_path']:
            raise ValueError('Missing reason or evidence path')
    result = []
    for row in rows:
        extra = dict.fromkeys(FIELDS, '')
        if row['candidate_url_id'] in checked:
            review = checked[row['candidate_url_id']]
            extra.update(zip(FIELDS, [
                'True', review['model'], review['decision'], review['rationale'],
                review['evidence_path'], review['evidence_sha256'], review['evidence_timestamp'],
                json.dumps(review['remaining_flags'], ensure_ascii=False), review['record_file'],
            ]))
        result.append({**row, **extra})
    assert all(all(new[k] == value for k, value in old.items())
               for old, new in zip(rows, result))
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--validation', type=Path, required=True)
    parser.add_argument('--reviews', type=Path, nargs='+', required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    if args.output.resolve() in {p.resolve() for p in [args.validation, *args.reviews]}:
        raise ValueError('Output must differ from inputs')
    with args.validation.open(newline='') as handle:
        reader = csv.DictReader(handle)
        original_fields = reader.fieldnames
        rows = list(reader)
    packets = [(p, json.loads(p.read_text())) for p in args.reviews]
    result = attach(rows, packets, digest(args.validation))
    with args.output.open('x', newline='') as handle:
        writer = csv.DictWriter(handle, fieldnames=original_fields + FIELDS)
        writer.writeheader()
        writer.writerows(result)
    with args.output.open(newline='') as handle:
        saved = list(csv.DictReader(handle))
    if saved != result:
        raise ValueError('Output readback mismatch')
    summary = {
        'input_hashes': {str(p): digest(p) for p in [args.validation, *args.reviews]},
        'output_sha256': digest(args.output), 'rows_before': len(rows), 'rows_after': len(saved),
        'original_fields_preserved': True, 'preserved_fields': original_fields,
        'matched_keys': sorted(r['candidate_url_id'] for r in result if r['ai_capture_reviewed'] == 'True'),
        'decision_counts': dict(Counter(r['ai_capture_review_decision'] for r in result
                                        if r['ai_capture_reviewed'] == 'True')),
        'human_approvals_added': 0,
    }
    summary_path = args.output.with_suffix('.summary.json')
    with summary_path.open('x') as handle:
        json.dump(summary, handle, indent=2)
        handle.write('\n')
    print(json.dumps(summary, indent=2))


if __name__ == '__main__':
    main()
