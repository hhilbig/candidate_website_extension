#!/usr/bin/env python3
"""Authenticate complete independent Stage A outputs at their origin."""
import argparse
import hashlib
import json
from pathlib import Path
import sys


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--project-root', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    sys.path.insert(0, str(args.project_root.resolve()))
    from scripts import build_candidate_endorsement_round4_development_inputs as builder
    from scripts.hive import extract_candidate_endorsement_round4_development_vllm as runner
    expected = '0a962b41e022a68eebbffe01c3f406cf41bbc55b4710251102ae6ba92f79f1d2'
    _, rows, authenticated, provenance = builder.authenticate_fresh_stage_a_run(
        args.project_root / 'input', args.project_root / 'output_stage_a',
        expected_input_manifest_sha256=expected,
    )
    if authenticated['manifest']['sample_round'] != 4:
        raise ValueError('Origin outputs do not belong to the independent sample.')
    if len(rows) != 292 or len(authenticated['parents']) != 40:
        raise ValueError('Origin outputs do not contain the complete frozen sample.')
    metadata = builder.strict_json(args.project_root / 'output_stage_a' / 'run_metadata.json')
    if metadata.get('slurm_job_id') != '23312106':
        raise ValueError('Origin metadata belongs to a different Slurm job.')
    result = {
        'version': 'candidate_endorsement_independent40_stage_a_origin_v1',
        'job_id': '23312106', 'provenance': provenance,
        'processed_count': len(rows),
        'technical_failure_count': sum(row['technical_pass'] is not True for row in rows),
        'complete_run_inventory': True, 'human_reviewed': False,
        'builder_sha256': builder.sha256_file(Path(builder.__file__)),
        'runner_sha256': builder.sha256_file(Path(runner.__file__)),
        'attestor_sha256': hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
    }
    data = (json.dumps(result, indent=2) + '\n').encode()
    if args.output.exists() and args.output.read_bytes() != data:
        raise ValueError('Refuse to overwrite a different origin attestation.')
    args.output.write_bytes(data)
    print(data.decode(), end='')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
