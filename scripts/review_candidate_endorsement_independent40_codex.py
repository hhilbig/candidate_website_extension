"""Prepare source-only inputs and validate explicit independent AI reviews."""
from __future__ import annotations

import argparse
import copy
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts import build_candidate_endorsement_independent40_stage_a as adapter
from scripts import build_candidate_endorsement_round4_human_review_app as app
from scripts import review_candidate_endorsement_codex as utilities

OUTPUT = ROOT / 'build/private_candidate_endorsement_independent40_codex_review_20260914a'
STAGE_A_SHA256 = '0a962b41e022a68eebbffe01c3f406cf41bbc55b4710251102ae6ba92f79f1d2'
AUTHORIZATIONS = {'human_truth': False, 'production_integration': False, 'full_expansion': False, 'release': False}
source_lines = utilities.source_lines
name_span = utilities.name_span
child = utilities.child


def coding_instructions():
    task = utilities.load_task()
    return {'version': 'independent40_codex_coding_reference_v1',
            'source_task_sha256': utilities.TASK_SHA256,
            'source_task_revision': task['task_revision'],
            'original_instructions': task['instructions'], 'original_contract': task['contract'],
            'review_mode': 'AI review, not human confirmation.',
            'ai_review_instructions': [
                'Inspect all complete source text. Do not treat deterministic visibility boundaries as semantic truth.',
                'Identify endorsements directed toward the focal candidate. Distinguish candidates whom the focal candidate endorses.',
                'Distinguish headings and affiliations from endorser identities.',
                'Retain one row per distinct identity occurrence. Repeated names at disjoint coordinates remain distinct.',
                'Distinct people may share a surname span. Preserve the approved source components in separate rows.',
                'Record unresolved public-support, date, shared-name, or endorsement-direction ambiguity as source-spanned AI uncertainty.',
                'Do not impose new date exclusions, numerical thresholds, or coding definitions.',
                'Keep the first pass blind to model seeds. Perform a separate seeded pass only after authenticated Stage B outputs exist.',
            ], 'human_reviewed': False, 'authorizations': AUTHORIZATIONS}


def authority():
    parents, _ = adapter.authenticated_parents(adapter.selection_helper.OUTPUT / 'selection.json', adapter.selection_helper.OUTPUT / 'selected_sources.json')
    source_parents, manifest, actual_hash = app.load_stage_a_source_artifact(adapter.OUTPUT, expected_manifest_sha256=STAGE_A_SHA256)
    original = {parent['parent_document_id']: parent for parent in parents}
    if len(source_parents) != 40 or set(original) != {parent['parent_document_id'] for parent in source_parents}:
        raise ValueError('Independent source closure differs.')
    for parent in source_parents:
        if parent['parent_source_text'] != original[parent['parent_document_id']]['parent_source_text']:
            raise ValueError('Independent source bytes differ.')
    return sorted(parents, key=lambda parent: parent['parent_document_id']), {
        'selection_sha256': adapter.runner.INDEPENDENT_SAMPLE4_BINDING['selection_sha256'],
        'selected_sources_sha256': adapter.runner.INDEPENDENT_SAMPLE4_BINDING['selected_sources_sha256'],
        'stage_a_manifest_sha256': actual_hash,
        'selection_spec_sha256': adapter.runner.INDEPENDENT_SAMPLE4_BINDING['selection_spec_sha256'],
        'source_utility_sha256': utilities.digest(Path(utilities.__file__).read_bytes()),
        'sample_round': manifest['sample_round'], 'seed': adapter.runner.INDEPENDENT_SAMPLE4_BINDING['seed'],
    }


def prepared_parent(index, parent):
    text = parent['parent_source_text']
    return {'parent_index': index, 'parent_document_id': parent['parent_document_id'],
            'metadata': {key: copy.deepcopy(parent[key]) for key in ('candidate_cycle_id', 'candidate_icpsr', 'state', 'office', 'year', 'party', 'source_urls')},
            'source_text': text, 'source_text_sha256': parent['extraction_source_text_sha256'],
            'lines': source_lines(text), 'human_reviewed': False}


def prepare(output_dir=OUTPUT):
    parents, provenance = authority()
    instructions = coding_instructions()
    utilities.write_json(output_dir / 'instructions.json', instructions)
    entries = []
    templates = []
    for index, parent in enumerate(parents, 1):
        value = prepared_parent(index, parent)
        path = output_dir / 'source' / f'parent_{index:02d}.json'
        utilities.write_json(path, value)
        entries.append({'parent_index': index, 'parent_document_id': value['parent_document_id'],
                        'source_text_sha256': value['source_text_sha256'],
                        'file': f'source/parent_{index:02d}.json', 'file_sha256': utilities.digest(path.read_bytes())})
        templates.append({'parent_index': index, 'parent_document_id': value['parent_document_id'],
                          'source_text_sha256': value['source_text_sha256'],
                          'review_status': 'needs_review', 'parent_disposition': 'needs_review',
                          'parent_confirmed': False, 'complete_source_reviewed': False,
                          'ai_reviewed': False, 'human_reviewed': False, 'children': [], 'uncertainties': []})
    manifest = {'version': 'independent40_codex_source_only_inputs_v1', 'parent_count': 40,
                'source_only_no_model_seeds': True, 'review_status': 'ai_review_pending',
                'human_reviewed': False, 'ai_reviewed': False, 'model_calls': False,
                'authorizations': AUTHORIZATIONS, 'provenance': provenance, 'parents': entries}
    utilities.write_json(output_dir / 'manifest.json', manifest)
    utilities.write_json(output_dir / 'instruction_provenance.json', {
        'instructions_sha256': utilities.digest((output_dir / 'instructions.json').read_bytes()),
        'source_task_sha256': utilities.TASK_SHA256,
        'source_manifest_sha256': utilities.digest((output_dir / 'manifest.json').read_bytes()),
        'human_reviewed': False, 'authorizations': AUTHORIZATIONS,
    })
    utilities.write_json(output_dir / 'review_template.json', {'version': 'independent40_pending_review_template_v1',
                         'manifest_sha256': utilities.digest((output_dir / 'manifest.json').read_bytes()),
                         'human_reviewed': False, 'ai_reviewed': False, 'authorizations': AUTHORIZATIONS,
                         'parents': templates})
    return manifest


def read_parent(index, output_dir=OUTPUT):
    if type(index) is not int or not 1 <= index <= 40:
        raise ValueError('Parent index is outside the independent sample.')
    parents, provenance = authority()
    if adapter.selection_helper.prior.strict_json(output_dir / 'instructions.json') != coding_instructions():
        raise ValueError('The approved coding reference differs.')
    expected_instruction_provenance = {
        'instructions_sha256': utilities.digest((output_dir / 'instructions.json').read_bytes()),
        'source_task_sha256': utilities.TASK_SHA256,
        'source_manifest_sha256': utilities.digest((output_dir / 'manifest.json').read_bytes()),
        'human_reviewed': False, 'authorizations': AUTHORIZATIONS,
    }
    if adapter.selection_helper.prior.strict_json(output_dir / 'instruction_provenance.json') != expected_instruction_provenance:
        raise ValueError('Coding reference provenance differs.')
    manifest = adapter.selection_helper.prior.strict_json(output_dir / 'manifest.json')
    if (set(manifest) != {'version', 'parent_count', 'source_only_no_model_seeds', 'review_status', 'human_reviewed', 'ai_reviewed', 'model_calls', 'authorizations', 'provenance', 'parents'}
            or manifest.get('version') != 'independent40_codex_source_only_inputs_v1'
            or manifest.get('provenance') != provenance or manifest.get('parent_count') != 40
            or manifest.get('authorizations') != AUTHORIZATIONS
            or manifest.get('human_reviewed') is not False or manifest.get('ai_reviewed') is not False
            or manifest.get('source_only_no_model_seeds') is not True
            or manifest.get('review_status') != 'ai_review_pending' or manifest.get('model_calls') is not False
            or len(manifest.get('parents', [])) != 40):
        raise ValueError('Source-only manifest authority differs.')
    # Authenticate the complete inventory before exposing any source file.
    for number, original in enumerate(parents, 1):
        entry = manifest['parents'][number - 1]
        expected_file = f'source/parent_{number:02d}.json'
        if (set(entry) != {'parent_index', 'parent_document_id', 'source_text_sha256', 'file', 'file_sha256'}
                or entry.get('file') != expected_file or entry.get('parent_index') != number):
            raise ValueError('Source inventory order differs.')
        path = output_dir / expected_file
        if utilities.digest(path.read_bytes()) != entry.get('file_sha256'):
            raise ValueError('Prepared source file hash differs.')
        value = adapter.selection_helper.prior.strict_json(path)
        if (value != prepared_parent(number, original)
                or entry.get('parent_document_id') != original['parent_document_id']
                or entry.get('source_text_sha256') != original['extraction_source_text_sha256']):
            raise ValueError('Prepared source identity or bytes differ.')
        if number == index:
            chosen = value
    return chosen


def validate_review(review, parent):
    utilities.validate_review(review, parent)
    if (set(review) != {'parent_index', 'parent_document_id', 'source_text_sha256', 'review_status', 'human_reviewed', 'complete_source_reviewed', 'reviewer_id', 'reviewer_model', 'children', 'uncertainties', 'notes', 'authorizations'}
            or review.get('authorizations') != AUTHORIZATIONS):
        raise ValueError('Review authority differs.')
    if type(review.get('reviewer_model')) is not str or not review['reviewer_model'].strip():
        raise ValueError('Review model provenance is missing.')
    for row in review['children']:
        if row.get('endorsement_status') not in {'accepted_by_ai_review', 'uncertain'}:
            raise ValueError('Child lacks an explicit AI review status.')
        if any(row.get(key, False) is not False for key in ('human_reviewed', 'human_confirmed', 'confirmed')):
            raise ValueError('A child claims human review or confirmation.')
    return review


def write_review(index, children, reviewer_id, *, pass_name, complete_source_reviewed,
                 uncertainties=None, notes='', output_dir=OUTPUT,
                 reviewer_model='inherited_primary_codex_model'):
    if pass_name not in {'blind', 'seed_audit', 'adjudicated'}:
        raise ValueError('Unknown independent review pass.')
    if complete_source_reviewed is not True:
        raise ValueError('Reviewer must attest complete source coverage.')
    if type(reviewer_model) is not str or not reviewer_model.strip():
        raise ValueError('Reviewer model must identify the actual Codex reviewer.')
    parent = read_parent(index, output_dir)
    review = {key: parent[key] for key in ('parent_index', 'parent_document_id', 'source_text_sha256')}
    review.update(review_status='ai_reviewed', human_reviewed=False, complete_source_reviewed=True,
                  reviewer_id=reviewer_id, reviewer_model=reviewer_model,
                  children=copy.deepcopy(children), uncertainties=copy.deepcopy(uncertainties or []),
                  notes=notes, authorizations=AUTHORIZATIONS)
    validate_review(review, parent)
    utilities.write_json(output_dir / pass_name / f'parent_{index:02d}.json', review)
    return review


def freeze_blind_reference(output_dir=OUTPUT):
    """Freeze all explicit blind judgments before any seeded comparison."""
    read_parent(1, output_dir)  # Authenticate the entire prepared source inventory.
    expected_files = {f'parent_{index:02d}.json' for index in range(1, 41)}
    folder = output_dir / 'blind'
    if {path.name for path in folder.iterdir()} != expected_files:
        raise ValueError('Blind review does not contain exactly all 40 parents.')
    responses, inventory = [], []
    counts = {'parents': 40, 'accepted_children': 0, 'uncertain_children': 0,
              'uncertainty_blocks': 0, 'ai_zero_parents': 0}
    for index in range(1, 41):
        parent = adapter.selection_helper.prior.strict_json(output_dir / 'source' / f'parent_{index:02d}.json')
        path = folder / f'parent_{index:02d}.json'
        if path.is_symlink() or not path.is_file():
            raise ValueError('Blind review file is not a regular file.')
        value = validate_review(adapter.selection_helper.prior.strict_json(path), parent)
        expected_reviewer = 'independent_blind_01_20' if index <= 20 else 'independent_blind_21_40'
        if value['reviewer_id'] != expected_reviewer or value['reviewer_model'] != 'gpt-5.6-sol':
            raise ValueError('Blind review has unexpected reviewer provenance.')
        responses.append({**copy.deepcopy(value), 'metadata': parent['metadata']})
        inventory.append({'parent_index': index, 'file': f'blind/{path.name}',
                          'sha256': utilities.digest(path.read_bytes())})
        counts['accepted_children'] += sum(row['endorsement_status'] == 'accepted_by_ai_review' for row in value['children'])
        counts['uncertain_children'] += sum(row['endorsement_status'] == 'uncertain' for row in value['children'])
        counts['uncertainty_blocks'] += len(value['uncertainties'])
        counts['ai_zero_parents'] += not value['children'] and not value['uncertainties']
    result = {'version': 'independent40_blind_ai_reference_v1',
              'review_status': 'complete_blind_ai_review_not_human_truth',
              'source_manifest_sha256': utilities.digest((output_dir / 'manifest.json').read_bytes()),
              'instructions_sha256': utilities.digest((output_dir / 'instructions.json').read_bytes()),
              'review_inventory': inventory, 'counts': counts, 'responses': responses,
              'human_reviewed': False, 'authorizations': AUTHORIZATIONS,
              'interpretation': 'Blind AI judgments are provisional. Uncertainty is retained; this is not human-validated accuracy.'}
    utilities.write_json(output_dir / 'blind_reviewed_reference.json', result)
    return result


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('mode', choices=['prepare', 'freeze-blind'])
    args = parser.parse_args()
    if args.mode == 'prepare':
        manifest = prepare()
        print(json.dumps({'parent_count': manifest['parent_count'], 'source_only': True,
                          'output_dir': str(OUTPUT), 'human_reviewed': False}))
    else:
        result = freeze_blind_reference()
        print(json.dumps({'counts': result['counts'], 'human_reviewed': False}))
