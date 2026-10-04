"""Authenticate two independent AI passes and retain private adjudication evidence."""
from __future__ import annotations

import argparse
import copy
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts import review_candidate_endorsement_independent40_codex as review

OUTPUT = ROOT / 'build/private_candidate_endorsement_independent40_codex_comparison_20260914a'
BLIND_SHA256 = '8d6f5dbad5fbd2dc392bc1ad45f0048bad5fac4d9552d5f677e08d293cebcb79'
strict_json = review.adapter.selection_helper.prior.strict_json
digest = review.utilities.digest


def reject_human_claims(value):
    if isinstance(value, dict):
        for key, item in value.items():
            if key in {'human_reviewed', 'human_confirmed', 'confirmed', 'human_truth'} and item is not False:
                raise ValueError('AI pass claims human review or confirmation.')
            reject_human_claims(item)
    elif isinstance(value, list):
        for item in value:
            reject_human_claims(item)


def pass_reference(pass_name, parents, source_dir):
    """Reconstruct a reference from exactly 40 authenticated explicit reviews."""
    folder = source_dir / pass_name
    expected = {f'parent_{index:02d}.json' for index in range(1, 41)}
    if folder.is_symlink() or not folder.is_dir() or {path.name for path in folder.iterdir()} != expected:
        raise ValueError(f'{pass_name} review does not contain exactly all 40 parents.')
    responses, inventory = [], []
    counts = {'parents': 40, 'accepted_children': 0, 'uncertain_children': 0,
              'uncertainty_blocks': 0, 'ai_zero_parents': 0}
    for index, parent in enumerate(parents, 1):
        path = folder / f'parent_{index:02d}.json'
        if path.is_symlink() or not path.is_file():
            raise ValueError('Review file is not a regular file.')
        value = review.validate_review(strict_json(path), parent)
        reject_human_claims(value)
        expected_reviewer = f'independent_{pass_name}_' + ('01_20' if index <= 20 else '21_40')
        if value['reviewer_id'] != expected_reviewer or value['reviewer_model'] != 'gpt-5.6-sol':
            raise ValueError(f'{pass_name} review has unexpected reviewer provenance.')
        responses.append({**copy.deepcopy(value), 'metadata': parent['metadata']})
        inventory.append({'parent_index': index, 'file': f'{pass_name}/{path.name}',
                          'sha256': digest(path.read_bytes())})
        counts['accepted_children'] += sum(row['endorsement_status'] == 'accepted_by_ai_review' for row in value['children'])
        counts['uncertain_children'] += sum(row['endorsement_status'] == 'uncertain' for row in value['children'])
        counts['uncertainty_blocks'] += len(value['uncertainties'])
        counts['ai_zero_parents'] += not value['children'] and not value['uncertainties']
    return {'version': f'independent40_{pass_name}_ai_reference_v1',
            'review_status': f'complete_{pass_name}_ai_review_not_human_truth',
            'source_manifest_sha256': digest((source_dir / 'manifest.json').read_bytes()),
            'instructions_sha256': digest((source_dir / 'instructions.json').read_bytes()),
            'review_inventory': inventory, 'counts': counts, 'responses': responses,
            'human_reviewed': False, 'authorizations': copy.deepcopy(review.AUTHORIZATIONS),
            'interpretation': ('Blind AI judgments are provisional. Uncertainty is retained; this is not human-validated accuracy.'
                               if pass_name == 'blind' else
                               'Seed-audit AI judgments are provisional. Uncertainty is retained; this is not human-validated accuracy.')}


def disposition(value):
    if any(row['endorsement_status'] == 'accepted_by_ai_review' for row in value['children']):
        return 'contains_children'
    return 'unclear' if value['children'] or value['uncertainties'] else 'true_zero'


def span_evidence(parent, spans):
    return [{'source_span': span, 'source_text': parent['source_text'][span[0]:span[1]]} for span in spans]


def child_evidence(parent, row):
    return {**copy.deepcopy(row), 'name_evidence': span_evidence(parent, row['name_spans']),
            'support_evidence': span_evidence(parent, row['support_spans']), 'human_reviewed': False}


def compare_values(parents, blind, seed):
    counts = {'parents': 40, 'blind_children': 0, 'seed_audit_children': 0,
              'exact_identity_agreements': 0, 'exact_identity_status_agreements': 0,
              'blind_only_identities': 0, 'seed_audit_only_identities': 0,
              'status_disagreements': 0, 'parent_uncertainty_disagreements': 0,
              'parent_disposition_disagreements': 0, 'parents_with_disagreements': 0,
              'parents_with_retained_uncertainty': 0, 'parents_requiring_adjudication': 0}
    comparisons, evidence = [], []
    for parent, a, b in zip(parents, blind['responses'], seed['responses'], strict=True):
        aa = {review.utilities.identity(row): row for row in a['children']}
        bb = {review.utilities.identity(row): row for row in b['children']}
        shared = aa.keys() & bb.keys()
        changed = {key for key in shared if aa[key]['endorsement_status'] != bb[key]['endorsement_status']}
        only_a, only_b = aa.keys() - bb.keys(), bb.keys() - aa.keys()
        uncertainties_differ = a['uncertainties'] != b['uncertainties']
        dispositions_differ = disposition(a) != disposition(b)
        differs = bool(only_a or only_b or changed or uncertainties_differ or dispositions_differ)
        unresolved = bool(a['uncertainties'] or b['uncertainties'] or
                          any(row['endorsement_status'] == 'uncertain' for row in a['children'] + b['children']))
        counts['blind_children'] += len(aa)
        counts['seed_audit_children'] += len(bb)
        counts['exact_identity_agreements'] += len(shared)
        counts['exact_identity_status_agreements'] += len(shared - changed)
        counts['blind_only_identities'] += len(only_a)
        counts['seed_audit_only_identities'] += len(only_b)
        counts['status_disagreements'] += len(changed)
        counts['parent_uncertainty_disagreements'] += uncertainties_differ
        counts['parent_disposition_disagreements'] += dispositions_differ
        counts['parents_with_disagreements'] += differs
        counts['parents_with_retained_uncertainty'] += unresolved
        counts['parents_requiring_adjudication'] += differs or unresolved
        item = {'parent_index': parent['parent_index'], 'parent_document_id': parent['parent_document_id'],
                'source_text_sha256': parent['source_text_sha256'],
                'blind_disposition': disposition(a), 'seed_audit_disposition': disposition(b),
                'identity_membership_differs': bool(only_a or only_b),
                'status_membership_differs': bool(only_a or only_b or changed), 'uncertainties_differ': uncertainties_differ,
                'disposition_differs': dispositions_differ, 'has_disagreement': differs,
                'has_retained_uncertainty': unresolved, 'requires_adjudication': differs or unresolved,
                'human_reviewed': False}
        comparisons.append(item)
        if differs or unresolved:
            evidence.append({**item, 'metadata': parent['metadata'],
                'source_file': f'source/parent_{parent["parent_index"]:02d}.json',
                'reviewer_ids': [a['reviewer_id'], b['reviewer_id']],
                'reviewer_models': [a['reviewer_model'], b['reviewer_model']],
                'blind_only': [child_evidence(parent, aa[key]) for key in sorted(only_a)],
                'seed_audit_only': [child_evidence(parent, bb[key]) for key in sorted(only_b)],
                'status_disagreements': [{'blind': child_evidence(parent, aa[key]),
                                          'seed_audit': child_evidence(parent, bb[key])} for key in sorted(changed)],
                'blind_uncertain_children': [child_evidence(parent, row) for row in a['children'] if row['endorsement_status'] == 'uncertain'],
                'seed_audit_uncertain_children': [child_evidence(parent, row) for row in b['children'] if row['endorsement_status'] == 'uncertain'],
                'blind_uncertainties': [{**copy.deepcopy(value), 'evidence': span_evidence(parent, value['source_spans'])} for value in a['uncertainties']],
                'seed_audit_uncertainties': [{**copy.deepcopy(value), 'evidence': span_evidence(parent, value['source_spans'])} for value in b['uncertainties']],
                'blind_notes': a['notes'], 'seed_audit_notes': b['notes']})
    return counts, comparisons, evidence


def compare(source_dir=review.OUTPUT, output_dir=OUTPUT, *, blind_sha256=BLIND_SHA256):
    source_dir, output_dir = Path(source_dir), Path(output_dir)
    if output_dir.exists() or output_dir.is_symlink():
        raise ValueError('Comparison output must be a new directory; no overwrites are permitted.')
    review.read_parent(1, source_dir)  # Authenticates all source files and coding provenance.
    parents = [strict_json(source_dir / 'source' / f'parent_{index:02d}.json') for index in range(1, 41)]
    frozen_path = source_dir / 'blind_reviewed_reference.json'
    if frozen_path.is_symlink() or not frozen_path.is_file() or digest(frozen_path.read_bytes()) != blind_sha256:
        raise ValueError('Frozen blind reference hash differs.')
    blind = pass_reference('blind', parents, source_dir)
    if strict_json(frozen_path) != blind:
        raise ValueError('Blind inventory differs from the frozen blind reference.')
    seed = pass_reference('seed_audit', parents, source_dir)
    counts, comparisons, evidence = compare_values(parents, blind, seed)
    output_dir.mkdir(parents=True, exist_ok=False)
    seed_path = output_dir / 'seed_audit_reviewed_reference.json'
    review.utilities.write_json(seed_path, seed)
    provenance = {'source_directory': str(source_dir.resolve().relative_to(ROOT)) if source_dir.resolve().is_relative_to(ROOT) else str(source_dir.resolve()),
                  'source_manifest_sha256': blind['source_manifest_sha256'],
                  'instructions_sha256': blind['instructions_sha256'],
                  'blind_reference_sha256': blind_sha256, 'seed_audit_reference_sha256': digest(seed_path.read_bytes()),
                  'comparison_script_sha256': digest(Path(__file__).read_bytes())}
    common = {'review_status': 'two_ai_passes_complete_adjudication_pending', 'human_reviewed': False,
              'authorizations': copy.deepcopy(review.AUTHORIZATIONS), 'provenance': provenance,
              'interpretation': 'Exact identity and status agreement between AI passes is not human-validated accuracy. No semantic correction or adjudication has been performed.'}
    report = {**common, 'version': 'independent40_codex_ai_pass_comparison_v1', 'counts': counts,
              'count_definitions': 'Identity is exact entity type and ordered name coordinates. Status membership adds endorsement_status. Disjoint occurrences remain distinct. Parent dispositions are derived without modifying reviews. Uncertainty blocks compare exact content; shared uncertainty is retained for adjudication.',
              'parents': comparisons}
    review.utilities.write_json(output_dir / 'comparison.json', report)
    review.utilities.write_json(output_dir / 'private_adjudication_evidence.json', {
        **common, 'version': 'independent40_codex_private_adjudication_evidence_v1',
        'parent_count': len(evidence), 'parents': evidence})
    return report


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source-dir', type=Path, default=review.OUTPUT)
    parser.add_argument('--output-dir', type=Path, default=OUTPUT)
    args = parser.parse_args()
    result = compare(args.source_dir, args.output_dir)
    print(json.dumps({'counts': result['counts'], 'output_dir': str(args.output_dir), 'human_reviewed': False}, indent=2))
