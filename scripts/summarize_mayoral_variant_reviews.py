"""Verify saved-capture reviews and report coverage without approving URLs."""
import argparse
import csv
from collections import Counter
import hashlib
import json
from pathlib import Path

DECISIONS = {
    'support_capture': 'supported',
    'support_campaign_with_target_caveat': 'caveated',
    'uncertain': 'uncertain',
    'uncertain_capture': 'uncertain',
    'uncertain_noncampaign_capture': 'uncertain',
    'reject_capture': 'capture_rejected',
    'reject_capture_wrong_office': 'capture_rejected',
    'reject_capture_noncampaign': 'capture_rejected',
    'reject_candidate_attribution': 'attribution_rejected',
}


def summarize(queue, packets, queue_hash, require_complete=False):
    lookup = {r['candidate_url_id']: r for r in queue}
    if len(lookup) != len(queue) or '' in lookup:
        raise ValueError('Duplicate or empty queue key')
    reviewed, flags, decisions = {}, Counter(), Counter()
    for filename, packet in packets:
        if packet.get('input_sha256') != queue_hash:
            raise ValueError('Review input hash mismatch')
        if not packet.get('reviewer') or not packet.get('model'):
            raise ValueError('Missing reviewer provenance')
        if packet.get('acceptance_changes') != 0:
            raise ValueError('Capture review must not change acceptance')
        for r in packet['records']:
            key = r['candidate_url_id']
            if key not in lookup or key in reviewed:
                raise ValueError('Unknown or duplicate review key')
            q = lookup[key]
            if (r.get('candidate_year_id') != q['candidate_year_id']
                    or r.get('capture_sha256') != q['evidence_sha256']
                    or r.get('capture_timestamp') != q['effective_timestamp']
                    or r.get('hash_verified') is not True):
                raise ValueError('Capture evidence mismatch')
            decision = r.get('decision')
            if decision not in DECISIONS or not r.get('reason'):
                raise ValueError('Missing or unknown decision')
            remaining = r.get('remaining_flags')
            if not isinstance(remaining, list) or not all(isinstance(x, str) for x in remaining):
                raise ValueError('Invalid remaining flags')
            reviewed[key] = dict(review_file=filename, decision=decision,
                                 reviewer=packet['reviewer'], model=packet['model'],
                                 review_date=packet.get('review_date', ''),
                                 reason=r['reason'], remaining_flags=remaining)
            decisions[DECISIONS[decision]] += 1
            flags.update(set(remaining))
    if require_complete and set(reviewed) != set(lookup):
        raise ValueError('Incomplete review coverage')
    return dict(version='mayoral-variant-review-summary-v1', queue_sha256=queue_hash,
                queue_rows=len(queue), reviewed_rows=len(reviewed),
                remaining_rows=len(queue)-len(reviewed), decision_counts=dict(decisions),
                remaining_flag_counts=dict(flags),
                unreviewed_keys=sorted(set(lookup)-set(reviewed)),
                attribution_rejection_keys=sorted(k for k, r in reviewed.items()
                                                  if r['decision']=='reject_candidate_attribution'),
                reviews=reviewed, acceptance_changes=0,
                interpretation='Review coverage is not URL equivalence, human approval, archive completeness, or verification of every capture. Hash verification is recorded by reviewers; this summary verifies evidence bindings.')


def review_rows(queue, report):
    """Keep every queue field; append distinct capture-review fields."""
    rows = []
    for original in queue:
        review = report['reviews'].get(original['candidate_url_id'])
        extra = dict(ai_capture_reviewed=bool(review), ai_capture_review_decision='',
                     ai_capture_review_reason='', ai_capture_review_model='',
                     ai_capture_reviewer='', ai_capture_review_date='',
                     ai_capture_review_file='', ai_capture_remaining_flags_json='[]')
        if review:
            extra.update(ai_capture_review_decision=review['decision'],
                         ai_capture_review_reason=review['reason'],
                         ai_capture_review_model=review['model'],
                         ai_capture_reviewer=review['reviewer'],
                         ai_capture_review_date=review['review_date'],
                         ai_capture_review_file=review['review_file'],
                         ai_capture_remaining_flags_json=json.dumps(review['remaining_flags']))
        if set(original) & set(extra):
            raise ValueError('Refusing to overwrite capture review fields')
        rows.append({**original, **extra})
    return rows


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--queue', type=Path, required=True)
    p.add_argument('--reviews', type=Path, nargs='+', required=True)
    p.add_argument('--output', type=Path, required=True)
    p.add_argument('--require-complete', action='store_true')
    p.add_argument('--rows-output', type=Path)
    args = p.parse_args()
    queue_bytes = args.queue.read_bytes()
    packets = [(str(f), json.loads(f.read_text())) for f in args.reviews]
    report = summarize(json.loads(queue_bytes), packets, hashlib.sha256(queue_bytes).hexdigest(), args.require_complete)
    report['review_file_sha256'] = {str(f): hashlib.sha256(f.read_bytes()).hexdigest() for f in args.reviews}
    report['script_sha256'] = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    if args.rows_output:
        rows = review_rows(json.loads(queue_bytes), report)
        if not rows:
            raise ValueError('Cannot export an empty queue')
        fields = list(dict.fromkeys(k for r in rows for k in r))
        with args.rows_output.open('x', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=fields)
            writer.writeheader()
            writer.writerows(rows)
        report['rows_output_sha256'] = hashlib.sha256(args.rows_output.read_bytes()).hexdigest()
    with args.output.open('x') as f:
        json.dump(report, f, indent=2)
        f.write('\n')
    print(json.dumps({k:report[k] for k in ['queue_rows','reviewed_rows','remaining_rows','decision_counts']}, indent=2))


if __name__ == '__main__':
    main()
