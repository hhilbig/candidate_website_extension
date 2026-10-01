"""Prepare a saved-evidence review queue without changing acceptance fields."""
import argparse
import csv
import hashlib
import json
import shutil
from collections import Counter
from datetime import datetime
from pathlib import Path


RELATIONS = {
    'saved_effective_exact_url': 'saved_effective_exact_url',
    'saved_syntactic_variant_only': 'saved_syntactic_variant_only',
    'different_path_query_or_host_only': 'different_path_query_or_host',
}
IDENTITY = ['candidate_url_id', 'candidate_year_id', 'candidate_contest_id',
            'full_name', 'geo_name', 'state_abb', 'year', 'month', 'exact_normalized_url']


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def read_csv(path):
    with path.open(newline='') as handle:
        reader = csv.DictReader(handle)
        if len(reader.fieldnames) != len(set(reader.fieldnames)):
            raise ValueError('Duplicate CSV columns')
        rows = list(reader)
    if any(None in row or any(value is None for value in row.values()) for row in rows):
        raise ValueError('Malformed CSV row')
    return rows


def unique(rows):
    result = {row['candidate_url_id']: row for row in rows}
    if len(result) != len(rows) or '' in result:
        raise ValueError('Duplicate or empty candidate URL key')
    return result


def prepare(audit, validation, candidates):
    unique(audit)
    checked, main = unique(validation), unique(candidates)
    queue, overlap = [], []
    for row in audit:
        key = row['candidate_url_id']
        candidate = main[key]
        for field in set(row) & set(candidate):
            if row[field] != candidate[field]:
                raise ValueError(f'Conflicting joined field: {key} {field}')
        if not all(candidate[field] for field in IDENTITY):
            raise ValueError('Missing candidate identity')
        if candidate['candidate_year_id'].split('__')[-1] != candidate['year']:
            raise ValueError('Candidate-year mismatch')
        relation = RELATIONS[row['classification']]
        evidence = json.loads(row['evidence_json'])[relation]
        required = ['raw_path', 'raw_sha256', 'effective_timestamp', 'effective_original_url',
                    'requested_original_url', 'election_reference_date', 'relation']
        if not all(evidence.get(field) for field in required):
            raise ValueError('Missing saved evidence')
        if evidence['relation'] != relation or evidence.get('raw_hash_check') != 'verified':
            raise ValueError('Invalid evidence relation or hash verification')
        if len(evidence['raw_sha256']) != 64 or any(c not in '0123456789abcdef' for c in evidence['raw_sha256']):
            raise ValueError('Invalid evidence hash')
        capture_date = datetime.strptime(evidence['effective_timestamp'], '%Y%m%d%H%M%S').date()
        reference = datetime.strptime(evidence['election_reference_date'], '%Y-%m-%d').date()
        offset = (capture_date - reference).days
        if abs(offset) != int(evidence['days_absolute']) or (abs(offset) <= 365) != evidence['within_365_days']:
            raise ValueError('Evidence date offset mismatch')
        pair = (evidence['raw_sha256'], evidence['effective_timestamp'])
        prior = checked.get(key)
        bases = []
        if prior:
            if prior['candidate_year_id'] != candidate['candidate_year_id']:
                raise ValueError('Validation candidate-year mismatch')
            bases = [basis for basis, fields in [
                ('primary', ('capture_content_sha256', 'capture_effective_timestamp')),
                ('supplement', ('supplement_raw_html_sha256', 'supplement_effective_timestamp'))]
                if pair == tuple(prior[field] for field in fields)]
        record = {field: candidate[field] for field in IDENTITY}
        extra = {'classification': row['classification'], 'relation': relation,
                 'evidence_path': evidence['raw_path'], 'evidence_sha256': pair[0],
                 'requested_original_url': evidence['requested_original_url'],
                 'effective_original_url': evidence['effective_original_url'],
                 'effective_timestamp': pair[1], 'election_reference_date': str(reference),
                 'days_signed': offset, 'days_absolute': abs(offset),
                 'within_365_days': abs(offset) <= 365,
                 'review_flags': [] if abs(offset) <= 365 else ['outside_365_day_window']}
        if set(record) & set(extra):
            raise ValueError('Conflicting output columns')
        record.update(extra)
        if bases:
            overlap.append({**record, 'validation_match_bases': bases})
        else:
            queue.append(record)
    return queue, overlap


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--audit', type=Path, required=True)
    parser.add_argument('--validation', type=Path, required=True)
    parser.add_argument('--candidates', type=Path, required=True)
    parser.add_argument('--output-dir', type=Path, required=True)
    args = parser.parse_args()
    inputs = [args.audit, args.validation, args.candidates]
    hashes = {str(path): digest(path) for path in inputs}
    audit, validation, candidates = [read_csv(path) for path in inputs]
    queue, overlap = prepare(audit, validation, candidates)
    if (len(audit), len(validation), len(queue), len(overlap)) != (521, 120, 460, 61):
        raise ValueError('Unexpected audit, validation, queue, or overlap counts')
    expected_queue = {'saved_syntactic_variant_only': 387, 'different_path_query_or_host_only': 73}
    expected_overlap = {'saved_effective_exact_url': 26, 'saved_syntactic_variant_only': 35}
    if dict(Counter(r['classification'] for r in queue)) != expected_queue:
        raise ValueError('Unexpected queue relation counts')
    if dict(Counter(r['classification'] for r in overlap)) != expected_overlap:
        raise ValueError('Unexpected overlap relation counts')
    args.output_dir.mkdir(parents=True, exist_ok=False)
    preserved = args.output_dir / 'saved_url_variant_audit.csv'
    shutil.copyfile(args.audit, preserved)
    outputs = [preserved]
    for name, records in [('review_queue.json', queue), ('validation_overlap.json', overlap)]:
        path = args.output_dir / name
        with path.open('x') as handle:
            json.dump(records, handle, ensure_ascii=False, indent=2)
            handle.write('\n')
        if json.loads(path.read_text()) != records:
            raise ValueError('Output readback mismatch')
        outputs.append(path)
    if digest(preserved) != hashes[str(args.audit)]:
        raise ValueError('Preserved audit hash mismatch')
    if hashes != {str(path): digest(path) for path in inputs}:
        raise ValueError('An input changed during execution')
    summary = {'input_sha256': hashes, 'script_sha256': digest(Path(__file__)),
               'output_sha256': {str(path): digest(path) for path in outputs},
               'audit_rows': len(audit), 'validation_rows': len(validation),
               'candidate_rows': len(candidates), 'queue_rows': len(queue),
               'overlap_rows': len(overlap), 'queue_classification_counts': expected_queue,
               'overlap_classification_counts': expected_overlap,
               'queue_within_365_days': sum(r['within_365_days'] for r in queue),
               'queue_outside_365_days': sum(not r['within_365_days'] for r in queue),
               'raw_html_copies': 0, 'acceptance_changes': 0,
               'evidence_verification': 'Audit hash metadata only; remote HTML files were not opened.'}
    with (args.output_dir / 'summary.json').open('x') as handle:
        json.dump(summary, handle, indent=2)
        handle.write('\n')
    print(json.dumps(summary, indent=2))


if __name__ == '__main__':
    main()
