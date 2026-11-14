#!/usr/bin/env python3
"""Build authenticated Stage A inputs for the frozen independent sample."""
from __future__ import annotations

import argparse
from collections import defaultdict
import copy
from pathlib import Path
import sys

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))
from scripts import build_candidate_endorsement_independent40_selection as selection_helper
from scripts import build_candidate_endorsement_round4_development_inputs as builder
from scripts.hive import extract_candidate_endorsement_round4_development_vllm as runner

OUTPUT = REPO / 'build/hive_candidate_extraction/v2_1_endorsement_independent40_stage_a_20260914b'


def authenticated_parents(selection_path, source_path):
    binding = runner.INDEPENDENT_SAMPLE4_BINDING
    selection = selection_helper.prior._authenticated_json(selection_path, binding['selection_sha256'], 'Independent selection')
    package = selection_helper.prior._authenticated_json(source_path, binding['selected_sources_sha256'], 'Independent selected sources')
    if (selection.get('sample_round') != 4 or selection.get('seed') != binding['seed']
            or selection.get('selection_spec') != selection_helper.SPEC
            or selection.get('selection_spec_sha256') != binding['selection_spec_sha256']
            or selection.get('selector_script_sha256') != selection_helper.prior.sha256_file(Path(selection_helper.__file__))
            or package.get('selection_sha256') != binding['selection_sha256']
            or any(package.get(key) is not False for key in ('human_reviewed', 'ai_reviewed', 'production_authorized', 'model_calls'))):
        raise ValueError('Independent source package scope or provenance differs.')
    pairs = {pid: component['source_text_sha256'] for component in selection['pilot_components'] for pid in component['parent_document_ids']}
    parents = package.get('parents')
    if type(parents) is not list or len(parents) != 40 or len({p['parent_document_id'] for p in parents}) != 40:
        raise ValueError('Independent parent closure differs.')
    provenance = {'independent_sample_binding': copy.deepcopy(binding), 'independent_sample_source_pairs': pairs}
    runner.validate_sample_round_binding(4, provenance, [p['parent_document_id'] for p in parents])
    # Authenticate the original export, then reconstruct only selected parents.
    full_rows, inventory_hash = selection_helper.prior.base.authenticated_full_rows()
    if inventory_hash != selection['authenticated_provenance']['source_content_inventory_sha256']:
        raise ValueError('Independent source inventory differs.')
    grouped = defaultdict(list)
    for row in full_rows:
        if row.get('route') == 'endorsements' and row.get('parent_document_id') in pairs:
            grouped[row['parent_document_id']].append(row)
    by_id = {p['parent_document_id']: p for p in parents}
    if set(grouped) != set(by_id):
        raise ValueError('Selected export closure differs.')
    authenticated = []
    for pid in sorted(by_id):
        parent = selection_helper.reconstruct_parent_inventory(grouped[pid])
        if parent != by_id[pid] or parent['extraction_source_text_sha256'] != pairs[pid]:
            raise ValueError('Independent source fields or line ledger differ.')
        authenticated.append(parent)
    return authenticated, provenance


def build_artifact(output_dir=OUTPUT):
    parents, provenance = authenticated_parents(selection_helper.OUTPUT / 'selection.json', selection_helper.OUTPUT / 'selected_sources.json')
    batches = builder.build_fresh_stage_a_batches(parents)
    provenance.update({'sample_selection_sha256': runner.INDEPENDENT_SAMPLE4_BINDING['selection_sha256'],
                       'sample_selection_spec_sha256': runner.INDEPENDENT_SAMPLE4_BINDING['selection_spec_sha256'],
                       'source_export_manifest_sha256': selection_helper.prior.sha256_file(REPO / builder.SOURCE_EXPORT / 'manifest.json'),
                       'sealed_reserve_source_text_reconstructed': False, 'ai_draft_used_in_prompt': False})
    manifest = builder._write_artifact(output_dir=output_dir, stage='line_decision', batches=batches,
                                       selected_parent_ids=sorted(p['parent_document_id'] for p in parents),
                                       provenance=provenance, chunk_size=60, sample_round=4)
    manifest_sha = selection_helper.prior.sha256_file(output_dir / 'manifest.json')
    runner.validate_input_manifest(output_dir / 'manifest.json', output_dir / 'chunks', stage='line_decision', expected_manifest_sha256=manifest_sha)
    return manifest, manifest_sha


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output-dir', type=Path, default=OUTPUT)
    args = parser.parse_args()
    manifest, digest = build_artifact(args.output_dir)
    print(builder.canonical_json({'manifest_sha256': digest, 'sample_round': manifest['sample_round'],
                                'parents': manifest['selected_parent_count'], 'batches': manifest['batch_count'],
                                'chunks': len(manifest['chunks']), 'inference_submitted': False}))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
