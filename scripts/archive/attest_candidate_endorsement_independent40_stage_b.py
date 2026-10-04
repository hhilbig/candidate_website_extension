#!/usr/bin/env python3
"""Authenticate complete independent Stage B outputs at their origin."""
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
    from scripts import build_candidate_endorsement_round4_human_review_app as app
    from scripts.hive import extract_candidate_endorsement_round4_development_vllm as runner
    a_hash = '0a962b41e022a68eebbffe01c3f406cf41bbc55b4710251102ae6ba92f79f1d2'
    a_inventory = '0c46b2033834a04423e2314671e176b15cc97aa2171027c5f8e15f5d2a850bbc'
    b_hash = '4f6113518037916c014c51a26ff7927b151915e2d0eaa4a5af5b493e13e61148'
    _, a_rows, authenticated, a_provenance = builder.authenticate_fresh_stage_a_run(
        args.project_root / 'input', args.project_root / 'output_stage_a',
        expected_input_manifest_sha256=a_hash,
        expected_output_inventory_sha256=a_inventory,
    )
    if (authenticated['manifest']['sample_round'] != 4
            or len(a_rows) != 292 or len(authenticated['parents']) != 40):
        raise ValueError('Stage A does not contain the complete independent sample.')
    _, rows, manifest, provenance = app.authenticate_fresh_stage_b_run(
        args.project_root / 'input_stage_b', args.project_root / 'output_stage_b',
        expected_input_manifest_sha256=b_hash,
        stage_a_input_manifest_sha256=a_hash,
        stage_a_output_provenance=a_provenance,
        selected_parent_ids=authenticated['manifest']['selected_parent_ids'],
        stage_a_sample_round=4,
    )
    if manifest['sample_round'] != 4 or len(rows) != 639:
        raise ValueError('Stage B does not contain the complete frozen sample.')
    metadata = builder.strict_json(args.project_root / 'output_stage_b' / 'run_metadata.json')
    if metadata.get('slurm_job_id') != '23312502':
        raise ValueError('Origin metadata belongs to a different Slurm job.')
    result = {
        'version': 'candidate_endorsement_independent40_stage_b_origin_v1',
        'job_id': '23312502', 'provenance': provenance,
        'stage_a_output_inventory_sha256': a_inventory,
        'processed_count': len(rows),
        'technical_failure_count': sum(row['technical_pass'] is not True for row in rows),
        'complete_run_inventory': True, 'human_reviewed': False,
        'builder_sha256': builder.sha256_file(Path(builder.__file__)),
        'converter_sha256': builder.sha256_file(Path(app.__file__)),
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
