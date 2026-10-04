"""Compare authenticated independent-40 seeds with frozen provisional AI reviews."""
from __future__ import annotations

import argparse
import hashlib
import json
from collections import Counter
from pathlib import Path
import re
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts import review_candidate_endorsement_independent40_codex as independent
from scripts.diagnose_candidate_endorsement_codex_review import (
    disagreement, multiset_comparison,
)
from scripts.evaluate_candidate_endorsement_round4_regression import strict_json

REFERENCE_SHA256 = '8d6f5dbad5fbd2dc392bc1ad45f0048bad5fac4d9552d5f677e08d293cebcb79'
METADATA_FIELDS = ('candidate_cycle_id', 'candidate_icpsr', 'state', 'office', 'year', 'party')
COMPARISONS = (('exact_name_coordinates_and_type', True),
               ('normalized_name_and_type_multiset', False))


def digest(raw):
    return hashlib.sha256(raw).hexdigest()


def pinned_json(path, expected_hash):
    path = Path(path)
    if not isinstance(expected_hash, str) or not re.fullmatch('[0-9a-f]{64}', expected_hash):
        raise ValueError('An explicit SHA256 pin is required')
    if path.is_symlink() or not path.is_file() or digest(path.read_bytes()) != expected_hash:
        raise ValueError(f'Pinned artifact bytes differ: {path}')
    return strict_json(path)


def no_human_claims(value):
    """Reject confirmation claims wherever they occur in a seed or review."""
    if isinstance(value, dict):
        for key, item in value.items():
            if key in {'human_reviewed', 'human_confirmed', 'human_parent_confirmed',
                       'parent_confirmed', 'confirmed'} and item is not False:
                raise ValueError('An AI diagnostic input claims human confirmation')
            no_human_claims(item)
    elif isinstance(value, list):
        for item in value:
            no_human_claims(item)


def closed_by_id(rows, label):
    if not isinstance(rows, list) or len(rows) != 40:
        raise ValueError(f'{label} must contain all 40 parents')
    result = {}
    for row in rows:
        key = row.get('parent_document_id')
        if not isinstance(key, str) or not key or key in result:
            raise ValueError(f'{label} has invalid or duplicate parent identity')
        result[key] = row
    return result


def load_reference(reference_dir=independent.OUTPUT):
    """Authenticate frozen reference, instructions, sources and 40 explicit reviews."""
    reference_dir = Path(reference_dir)
    reference = pinned_json(reference_dir / 'blind_reviewed_reference.json', REFERENCE_SHA256)
    no_human_claims(reference)
    if (reference.get('version') != 'independent40_blind_ai_reference_v1'
            or reference.get('review_status') != 'complete_blind_ai_review_not_human_truth'
            or reference.get('human_reviewed') is not False
            or reference.get('authorizations') != independent.AUTHORIZATIONS):
        raise ValueError('Frozen reference authority differs')
    manifest = pinned_json(reference_dir / 'manifest.json', reference['source_manifest_sha256'])
    instructions = pinned_json(reference_dir / 'instructions.json', reference['instructions_sha256'])
    no_human_claims(manifest)
    no_human_claims(instructions)
    provenance = manifest.get('provenance', {})
    if (manifest.get('parent_count') != 40
            or manifest.get('source_only_no_model_seeds') is not True
            or manifest.get('model_calls') is not False
            or manifest.get('authorizations') != independent.AUTHORIZATIONS
            or provenance.get('sample_round') != 4
            or provenance.get('stage_a_manifest_sha256') != independent.STAGE_A_SHA256
            or instructions.get('authorizations') != independent.AUTHORIZATIONS):
        raise ValueError('Independent source manifest binding differs')
    instruction_provenance = strict_json(reference_dir / 'instruction_provenance.json')
    if instruction_provenance != {
            'instructions_sha256': reference['instructions_sha256'],
            'source_task_sha256': instructions['source_task_sha256'],
            'source_manifest_sha256': reference['source_manifest_sha256'],
            'human_reviewed': False, 'authorizations': independent.AUTHORIZATIONS}:
        raise ValueError('Instruction provenance differs')
    entries = closed_by_id(manifest['parents'], 'Source inventory')
    responses = closed_by_id(reference['responses'], 'Reference')
    inventory = reference.get('review_inventory')
    if not isinstance(inventory, list) or len(inventory) != 40:
        raise ValueError('Review inventory closure differs')
    inventory_by_index = {row['parent_index']: row for row in inventory}
    if set(inventory_by_index) != set(range(1, 41)):
        raise ValueError('Review inventory indices differ')
    if {path.name for path in (reference_dir / 'blind').iterdir()} != {
            f'parent_{index:02d}.json' for index in range(1, 41)}:
        raise ValueError('Explicit blind review file closure differs')
    sources = {}
    counts = Counter(parents=40, accepted_children=0, uncertain_children=0,
                     uncertainty_blocks=0, ai_zero_parents=0)
    for parent_id, entry in entries.items():
        index = entry['parent_index']
        if entry['file'] != f'source/parent_{index:02d}.json':
            raise ValueError('Source inventory filename differs')
        source = pinned_json(reference_dir / entry['file'], entry['file_sha256'])
        if (source.get('parent_document_id') != parent_id
                or source.get('parent_index') != index
                or source.get('source_text_sha256') != entry['source_text_sha256']
                or digest(source['source_text'].encode()) != entry['source_text_sha256']
                or source.get('human_reviewed') is not False):
            raise ValueError('Authenticated source identity or bytes differ')
        row = inventory_by_index[index]
        if row['file'] != f'blind/parent_{index:02d}.json':
            raise ValueError('Review inventory filename differs')
        review = pinned_json(reference_dir / row['file'], row['sha256'])
        independent.validate_review(review, source)
        no_human_claims(review)
        reviewer = 'independent_blind_01_20' if index <= 20 else 'independent_blind_21_40'
        if review['reviewer_id'] != reviewer or review['reviewer_model'] != 'gpt-5.6-sol':
            raise ValueError('Explicit review model provenance differs')
        if responses.get(parent_id) != {**review, 'metadata': source['metadata']}:
            raise ValueError('Frozen response differs from explicit review file')
        sources[parent_id] = source
        counts['accepted_children'] += sum(c['endorsement_status'] == 'accepted_by_ai_review' for c in review['children'])
        counts['uncertain_children'] += sum(c['endorsement_status'] == 'uncertain' for c in review['children'])
        counts['uncertainty_blocks'] += len(review['uncertainties'])
        counts['ai_zero_parents'] += not review['children'] and not review['uncertainties']
    if dict(counts) != reference['counts'] or set(sources) != set(responses):
        raise ValueError('Frozen reference counts or source closure differ')
    # Authenticate the separately pinned round-four input, including its sample
    # binding and source pairs. No model calls or remote access occur here.
    stage_sources, stage_manifest, _ = independent.app.load_stage_a_source_artifact(
        independent.adapter.OUTPUT,
        expected_manifest_sha256=independent.STAGE_A_SHA256)
    stage_by_id = closed_by_id(stage_sources, 'Authenticated Stage-A sources')
    if stage_manifest['sample_round'] != 4 or set(stage_by_id) != set(sources):
        raise ValueError('Independent Stage-A sample binding differs')
    for parent_id, source in sources.items():
        original = stage_by_id[parent_id]
        if (original['parent_source_text'] != source['source_text']
                or original['extraction_source_text_sha256'] != source['source_text_sha256']
                or {key: original[key] for key in METADATA_FIELDS} != {
                    key: source['metadata'][key] for key in METADATA_FIELDS}):
            raise ValueError('Frozen source differs from authenticated independent Stage-A source')
    return reference, sources, manifest, instructions


def validate_task(task, sources, manifest, instructions):
    no_human_claims(task)
    parents = closed_by_id(task.get('parents'), 'Converted task')
    provenance = task.get('input_provenance', {})
    if (task.get('parent_count') != 40 or set(parents) != set(sources)
            or not isinstance(task.get('authorizations'), dict)
            or not task['authorizations'] or any(value is not False for value in task['authorizations'].values())
            or provenance.get('stage_a_input_manifest_sha256') != manifest['provenance']['stage_a_manifest_sha256']
            or manifest['provenance'].get('sample_round') != 4
            or task.get('instructions') != instructions['original_instructions']
            or task.get('contract') != instructions['original_contract']):
        raise ValueError('Converted independent task binding or authority differs')
    for parent_id, parent in parents.items():
        source = sources[parent_id]
        text = source['source_text']
        if (parent.get('parent_source_text') != text
                or parent.get('source_text_sha256') != source['source_text_sha256']
                or parent.get('source_n_chars') != len(text)
                or parent.get('metadata') != {key: source['metadata'][key] for key in METADATA_FIELDS}
                or parent.get('human_parent_confirmed') is not False):
            raise ValueError('Converted task full source identity or bytes differ')
        children = parent.get('ai_children')
        blocks = parent.get('ai_unresolved_blocks')
        if not isinstance(children, list) or not isinstance(blocks, list):
            raise ValueError('Converted task seed arrays differ')
        seen = set()
        for child in children:
            if child.get('entity_type') not in {'individual', 'organization'} or child.get('human_confirmed') is not False:
                raise ValueError('Prediction authority or entity type differs')
            independent.utilities.validate_spans(child.get('name_spans'), text, 'prediction name')
            support = child.get('support_spans')
            if not isinstance(support, list):
                raise ValueError('Prediction support spans missing')
            if support:
                independent.utilities.validate_spans(support, text, 'prediction support')
            key = independent.utilities.identity(child)
            if key in seen:
                raise ValueError('Duplicate prediction occurrence')
            seen.add(key)
        block_ids = set()
        for block in blocks:
            block_id = block.get('unresolved_block_id')
            if not isinstance(block_id, str) or not block_id or block_id in block_ids or block.get('human_reviewed') is not False:
                raise ValueError('Unresolved block identity or authority differs')
            independent.utilities.validate_spans([block.get('block_span')], text, 'unresolved block')
            block_ids.add(block_id)
    if (provenance.get('ai_child_count') != sum(len(p['ai_children']) for p in parents.values())
            or provenance.get('ai_unresolved_block_count') != sum(len(p['ai_unresolved_blocks']) for p in parents.values())):
        raise ValueError('Converted task seed counts differ')
    return parents


def build_diagnostic(task, reference, sources, manifest, instructions, task_hash):
    parents_by_id = validate_task(task, sources, manifest, instructions)
    responses = closed_by_id(reference['responses'], 'Reference')
    if set(responses) != set(parents_by_id):
        raise ValueError('Reference and task source closure differs')
    aggregates = {label: Counter() for label, _ in COMPARISONS}
    parents, relations = [], Counter()
    for parent_id, source_parent in sorted(sources.items()):
        original, response = parents_by_id[parent_id], responses[parent_id]
        source = source_parent['source_text']
        predictions, expected = original['ai_children'], response['children']
        metrics = {}
        for label, coordinates in COMPARISONS:
            metrics[label] = multiset_comparison(source, predictions, expected, coordinates=coordinates)
            aggregates[label].update({key: value for key, value in metrics[label].items() if type(value) is int})
        predicted_keys = {independent.utilities.identity(c) for c in predictions}
        expected_keys = {independent.utilities.identity(c) for c in expected}
        missing = [disagreement(source, c, predictions, original['ai_unresolved_blocks']) for c in expected if independent.utilities.identity(c) not in predicted_keys]
        extra = [disagreement(source, c, expected, original['ai_unresolved_blocks']) for c in predictions if independent.utilities.identity(c) not in expected_keys]
        relations.update('reference_only:' + c['comparison_relation'] for c in missing)
        relations.update('prediction_only:' + c['comparison_relation'] for c in extra)
        parents.append({'parent_index': source_parent['parent_index'], 'parent_document_id': parent_id,
                        'metadata': source_parent['metadata'], 'source_text_sha256': source_parent['source_text_sha256'],
                        'metrics': metrics, 'reference_only': missing, 'prediction_only': extra,
                        'ai_uncertainties': response['uncertainties'],
                        'unresolved_extraction_blocks': original['ai_unresolved_blocks']})
    comparison = {}
    for label, counter in aggregates.items():
        counts = dict(counter)
        a, u, n = counts['prediction_matches_ai_accepted'], counts['prediction_matches_ai_uncertain'], counts['prediction_count']
        accepted = counts['ai_accepted_reference_count']
        total = accepted + counts['ai_uncertain_reference_count']
        if a + u + counts['prediction_without_ai_reference'] != n:
            raise ValueError('Prediction partition does not close')
        counts.update(accepted_reference_match_rate=a / accepted if accepted else None,
                      all_provisional_reference_match_rate=(a + u) / total if total else None,
                      prediction_match_rate_all_provisional=(a + u) / n if n else None)
        comparison[label] = counts
    report = {'version': 'candidate_endorsement_independent40_ai_diagnostic_v1',
              'parent_count': 40, 'sample_round': 4, 'source_task_sha256': task_hash,
              'reference_sha256': REFERENCE_SHA256, 'reference_is_ai_not_human_truth': True,
              'human_reviewed': False, 'authorizations': independent.AUTHORIZATIONS,
              'input_provenance': task['input_provenance'], 'comparison': comparison,
              'relation_counts': dict(sorted(relations.items())),
              'unresolved_extraction_block_count': sum(len(p['unresolved_extraction_blocks']) for p in parents),
              'interpretation': 'Rates measure agreement with frozen provisional AI judgments across all 40 full pages, not human accuracy. Uncertain references are separate. Unmatched predictions are not established false endorsements.',
              'definitions': {'exact': 'Entity type and every name coordinate must agree.',
                              'normalized': 'Existing pipeline normalization; occurrence multisets remain within each parent.',
                              'accepted_priority': 'Match accepted references before uncertain references; count each prediction once.'}}
    return report, {'version': 'candidate_endorsement_independent40_private_disagreements_v1',
                    'human_reviewed': False, 'source_task_sha256': task_hash,
                    'reference_sha256': REFERENCE_SHA256, 'parents': parents}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--task', type=Path, required=True)
    parser.add_argument('--expected-task-sha256', required=True)
    parser.add_argument('--output-dir', type=Path, required=True)
    args = parser.parse_args()
    reference, sources, manifest, instructions = load_reference()
    task = pinned_json(args.task, args.expected_task_sha256)
    report, ledger = build_diagnostic(task, reference, sources, manifest, instructions, args.expected_task_sha256)
    outputs = (('diagnostic.json', report), ('private_disagreements.json', ledger))
    for filename, _ in outputs:
        path = args.output_dir / filename
        if path.exists():
            raise ValueError(f'Refusing to overwrite diagnostic artifact: {path}')
    for filename, value in outputs:
        path = args.output_dir / filename
        independent.utilities.write_json(path, value)
    print(json.dumps({'parent_count': 40, 'human_reviewed': False,
                      'comparison': report['comparison'], 'output_dir': str(args.output_dir)}))


if __name__ == '__main__':
    main()
