#!/usr/bin/env python3
"""Check exact tokenizer budgets for frozen independent extraction inputs."""
import argparse
import hashlib
import json
from pathlib import Path
import sys


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--project-root', type=Path, required=True)
    parser.add_argument('--input-dir', type=Path, required=True)
    parser.add_argument('--expected-manifest-sha256', required=True)
    parser.add_argument('--model-path', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--stage', choices=('line_decision', 'record_extraction'), default='line_decision')
    args = parser.parse_args()
    sys.path.insert(0, str(args.project_root.resolve()))
    from scripts.hive import extract_candidate_endorsement_round4_development_vllm as runner
    from transformers import AutoTokenizer

    if args.model_path.name != runner.MODEL_REVISION:
        raise ValueError('Tokenizer path does not identify the pinned model revision.')
    manifest, digest, chunks = runner.validate_input_manifest(
        args.input_dir / 'manifest.json', args.input_dir / 'chunks',
        stage=args.stage, expected_manifest_sha256=args.expected_manifest_sha256,
    )
    if manifest['sample_round'] != 4:
        raise ValueError('This check requires the frozen independent sample.')
    tokenizer = AutoTokenizer.from_pretrained(args.model_path, local_files_only=True)
    counts = [runner.prompt_token_count(tokenizer, runner.build_conversation(row))
              for chunk in chunks for row in runner.read_gzip_jsonl(chunk)]
    if len(counts) != manifest['batch_count']:
        raise ValueError('The prompt count differs from the authenticated manifest.')
    if not counts:
        raise ValueError('The authenticated input contains no prompts.')
    margin = runner.MAX_MODEL_LEN - runner.MAX_OUTPUT_TOKENS[args.stage] - max(counts)
    result = {
        'version': 'candidate_endorsement_independent40_token_budget_v1',
        'input_manifest_sha256': digest, 'batch_count': len(counts),
        'model_revision': runner.MODEL_REVISION,
        'max_prompt_tokens': max(counts), 'min_prompt_tokens': min(counts),
        'minimum_context_margin_tokens': margin,
        'required_context_margin_tokens': runner.CONTEXT_SAFETY_MARGIN_TOKENS,
        'budget_pass': margin >= runner.CONTEXT_SAFETY_MARGIN_TOKENS,
        'checker_sha256': hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        'human_reviewed': False, 'inference_submitted': False,
    }
    data = (json.dumps(result, indent=2) + '\n').encode()
    if args.output.exists() and args.output.read_bytes() != data:
        raise ValueError('Refuse to overwrite a different budget result.')
    args.output.write_bytes(data)
    print(data.decode(), end='')
    return 0 if result['budget_pass'] else 2


if __name__ == '__main__':
    raise SystemExit(main())
