"""Add AI decisions and saved-page evidence to the complete website table."""
import argparse
import csv
import hashlib
import json
from collections import Counter
from pathlib import Path


def unique(rows):
    result = {r['candidate_url_id']: r for r in rows}
    if len(result) != len(rows) or '' in result:
        raise ValueError('Duplicate or empty candidate URL key')
    return result


def integrate(main, validation, reviews):
    source = unique(main)
    checked = unique(validation)
    evidence = unique(reviews)
    reviewed = {k for k, r in checked.items() if r['ai_reviewed'] == 'True'}
    if set(checked) - set(source) or set(evidence) != reviewed:
        raise ValueError('Review keys do not match main table')
    fields = ['ai_reviewed', 'ai_review_model', 'ai_review_date', 'ai_review_decision',
              'ai_review_reason', 'ai_accepted_website', 'ai_review_evidence_sha256',
              'ai_review_evidence_path', 'ai_review_capture_timestamp',
              'ai_review_capture_url', 'ai_review_record_file']
    if any(set(row) & set(fields) for row in main):
        raise ValueError('Refusing to overwrite review columns')
    for key, row in checked.items():
        if row['candidate_year_id'] != source[key]['candidate_year_id']:
            raise ValueError('Candidate-year mismatch')
        if row['raw_url'] != source[key]['raw_url']:
            raise ValueError('URL mismatch')
    result = []
    for row in main:
        key = row['candidate_url_id']
        extra = dict.fromkeys(fields, '')
        extra.update(ai_reviewed='False', ai_accepted_website='False')
        if key in reviewed:
            review, v = evidence[key], checked[key]
            if (review['candidate_year_id'] != row['candidate_year_id']
                    or review['url'] != row['raw_url']
                    or review['capture_sha256'] != v['capture_content_sha256']
                    or review['capture_effective_timestamp'] != v['capture_effective_timestamp']
                    or review['decision'] != v['ai_review_decision']
                    or review['hash_verified'] is not True):
                raise ValueError('Evidence mismatch')
            for field in fields[:5]:
                extra[field] = v[field]
            extra.update(ai_accepted_website=str(v['ai_review_decision'] == 'accept'),
                         ai_review_evidence_sha256=review['capture_sha256'],
                         ai_review_evidence_path=review['capture_path'],
                         ai_review_capture_timestamp=review['capture_effective_timestamp'],
                         ai_review_capture_url=v['capture_effective_url'],
                         ai_review_record_file=review['record_file'])
        result.append({**row, **extra})
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--main', type=Path, required=True)
    parser.add_argument('--validation', type=Path, required=True)
    parser.add_argument('--reviews', type=Path, nargs='+', required=True)
    parser.add_argument('--output-dir', type=Path, required=True)
    args = parser.parse_args()
    paths = [args.main, args.validation, *args.reviews]
    hashes = {str(p): hashlib.sha256(p.read_bytes()).hexdigest() for p in paths}
    with args.main.open() as f:
        source = list(csv.DictReader(f))
    with args.validation.open() as f:
        validation = list(csv.DictReader(f))
    reviews = []
    for path in args.reviews:
        packet = json.loads(path.read_text())
        if packet['model'] != 'gpt-5.6-sol' or packet['review_type'] != 'ai_review':
            raise ValueError('Unexpected review model')
        reviews.extend({**r, 'record_file': str(path)} for r in packet['records'])
    result = integrate(source, validation, reviews)
    args.output_dir.mkdir(parents=True, exist_ok=True)
    output = args.output_dir / 'candidate_urls.csv'
    with output.open('x', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=list(result[0]))
        writer.writeheader()
        writer.writerows(result)
    assert all(all(row[k] == v for k, v in old.items()) for old, row in zip(source, result))
    accepted = [r for r in result if r['ai_accepted_website'] == 'True']
    summary = dict(input_hashes=hashes, main_rows_before=len(source), main_rows_after=len(result),
                   new_url_rows=0, original_fields_preserved=True,
                   ai_decisions=dict(Counter(r['ai_review_decision'] for r in result if r['ai_reviewed'] == 'True')),
                   ai_accepted_candidate_years=len({r['candidate_year_id'] for r in accepted}),
                   human_approvals_added=0, output_sha256=hashlib.sha256(output.read_bytes()).hexdigest())
    with (args.output_dir / 'integration_summary.json').open('x') as f:
        json.dump(summary, f, indent=2)
        f.write('\n')
    print(json.dumps(summary, indent=2))


if __name__ == '__main__':
    main()
