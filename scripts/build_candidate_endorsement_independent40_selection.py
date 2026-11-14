#!/usr/bin/env python3
"""Select the next private sample with the unchanged 32M/8Z contract."""
from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from contextlib import contextmanager
import gzip
import json
import math
from pathlib import Path
import sys

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))
from scripts import build_candidate_endorsement_recall40_sample_round3_selection as prior
from src.candidate_endorsement_line_ledger import (
    LINE_DECISION_SLOT_COUNT, _compound_anchor_expected_names,
    reconstruct_parent_inventory, sha256_json,
)

CENSUS = REPO / 'build/private_candidate_endorsement_codex_independent_pool_20260914a/pool_census.json'
CENSUS_SHA256 = '92592f0416faf0867f0e3964e9b57df2b0e9b11dc6d8019f7d46194f24ce272e'
SAMPLE_ROUND = 4
SEED = 20260827 + 1000 * (SAMPLE_ROUND - 1)
OUTPUT = REPO / 'build/private_candidate_endorsement_independent40_selection_20260914a'
SPEC = {
    **prior.SELECTION_SPEC,
    'version': 'candidate_endorsement_independent40_selection_spec_v1',
    'sample_round': SAMPLE_ROUND, 'seed': SEED,
    'pool_census_sha256': CENSUS_SHA256,
    'exclusions': 'Preserve all prior exclusions. Exclude current source components and candidate cycles. Close source components for those cycles.',
    'authorization': {'private_sample_assignment': True, 'source_task': True,
                      'hive_submission': False, 'human_truth': False,
                      'full_private_inference': False, 'release': False},
}
SPEC.pop('pipeline_round_three_reused_sample_round', None)


@contextmanager
def fresh_seed():
    """Use the new seed without changing the historical selector file."""
    old = prior.SEED
    prior.SEED = SEED
    try:
        yield
    finally:
        prior.SEED = old


def source_identity(rows):
    hashes = {str(row['extraction_source_text_sha256']) for row in rows}
    cycles = {str(row['candidate_cycle_id']) for row in rows}
    if len(hashes) != 1 or len(cycles) != 1:
        raise ValueError('Parent source or candidate identity differs across segments.')
    return next(iter(hashes)), next(iter(cycles))


def features(pid, rows, parent, legacy_count):
    """Apply the original feature definitions without changing any threshold."""
    text = str(parent['parent_source_text'])
    lines = [str(line['line_text']) for line in parent['lines']]
    nonempty = [line for line in lines if line.strip()]
    short = [line for line in nonempty if len(line) <= 160]
    model_lines = sum(bool(line['model_decision_required']) for line in parent['lines'])
    windows = math.ceil(model_lines / LINE_DECISION_SLOT_COUNT) if model_lines else 0
    consecutive = longest = 0
    for line in lines:
        consecutive = consecutive + 1 if line.strip() and len(line) <= 160 else 0
        longest = max(longest, consecutive)
    quote = any(len(line) >= 80 and (('“' in line and '”' in line) or line.count('"') >= 2) for line in lines)
    structure = 'Z' if legacy_count == 0 else 'M' if windows >= 2 else 'ONE'
    source_hash, cycle = source_identity(rows)
    return {
        'id': pid, 'source_hash': source_hash, 'candidate': cycle,
        'party': prior.base.party(rows[0].get('party')), 'year': rows[0].get('year'),
        'legacy': legacy_count, 'windows': windows, 'pilot': structure,
        'reserve': 'RZ' if structure == 'Z' else 'RM' if structure == 'M' else 'RS',
        'testimonial': bool(prior.base.SUPPORT.search(prior.base.URL.sub('', text)) or quote),
        'compound': any(_compound_anchor_expected_names(line) is not None for line in lines),
        'context': any(row.get('endorsement_context_provenance', {}).get('source_scope') == 'page_or_section_route_context' for row in rows),
        'url': bool(prior.base.URL.search(text)),
        'list': longest >= 5 or sum(bool(prior.base.BULLET.search(line)) for line in lines) >= 3,
        'density_ppm': round(1_000_000 * len(short) / max(1, len(nonempty))),
    }


def build_pool(reconstruct=reconstruct_parent_inventory):
    census = prior._authenticated_json(CENSUS, CENSUS_SHA256, 'Pool census')
    provenance = census['authenticated_provenance']
    for path_key, hash_key in [('task_path', 'task_sha256'), ('current_selection_path', 'current_selection_sha256'), ('selector_path', 'selector_sha256'), ('exclusion_helper_path', 'exclusion_helper_sha256'), ('legacy_output_path', 'legacy_output_sha256')]:
        if prior.sha256_file(REPO / provenance[path_key]) != provenance[hash_key]:
            raise ValueError(f'Authenticated input differs: {path_key}')
    rows, inventory_hash = prior.base.authenticated_full_rows()
    if inventory_hash != provenance['source_content_inventory_sha256']:
        raise ValueError('Source inventory differs.')
    grouped = defaultdict(list)
    for row in rows:
        if row.get('route') == 'endorsements':
            grouped[str(row['parent_document_id'])].append(row)
    forbidden, _, _ = prior.authenticated_forbidden_source_hashes(grouped)
    task = prior.strict_json(REPO / provenance['task_path'])
    current_ids = {p['parent_document_id'] for p in task['parents']}
    current_hashes = {p['source_text_sha256'] for p in task['parents']}
    current_cycles = {p['metadata']['candidate_cycle_id'] for p in task['parents']}
    identities = {pid: source_identity(source) for pid, source in grouped.items()}
    for parent in task['parents']:
        if identities[parent['parent_document_id']] != (parent['source_text_sha256'], parent['metadata']['candidate_cycle_id']):
            raise ValueError('Current sample identity differs.')
    cycle_hashes = {source_hash for source_hash, cycle in identities.values() if cycle in current_cycles}
    forbidden |= current_hashes | cycle_hashes
    ids = sorted(pid for pid, (source_hash, cycle) in identities.items() if source_hash not in forbidden and cycle not in current_cycles)
    hashes = sorted({identities[pid][0] for pid in ids})
    cycles = sorted({identities[pid][1] for pid in ids})
    checks = {
        'expanded_forbidden_source_hashes_sha256': sha256_json(sorted(forbidden)),
        'remaining_parent_ids_sha256': sha256_json(ids),
        'remaining_source_hashes_sha256': sha256_json(hashes),
        'remaining_candidate_cycle_ids_sha256': sha256_json(cycles),
        'current_parent_ids_sha256': sha256_json(sorted(current_ids)),
        'current_source_hashes_sha256': sha256_json(sorted(current_hashes)),
        'current_candidate_cycle_ids_sha256': sha256_json(sorted(current_cycles)),
    }
    if any(value != provenance[key] for key, value in checks.items()):
        raise ValueError('The exclusion boundary differs from the census.')
    legacy = {}
    with gzip.open(prior.base.LEGACY, 'rt', encoding='utf-8') as handle:
        for line in handle:
            row = json.loads(line)
            if row.get('route') == 'endorsements':
                legacy[str(row['parent_document_id'])] = len(row['records'])
    pool = []
    for pid in ids:
        parent = reconstruct(grouped[pid])
        if parent['extraction_source_text_sha256'] != identities[pid][0]:
            raise ValueError('Reconstructed source hash differs.')
        pool.append(features(pid, grouped[pid], parent, legacy[pid]))
    if len(pool) != 1622 or len(hashes) != 1579 or dict(Counter(p['pilot'] for p in pool)) != census['by_M_Z']:
        raise ValueError('The eligible pool differs from the census.')
    years = sorted(int(p['year']) for p in pool if p['year'] is not None)
    boundaries = [years[math.ceil(q * len(years)) - 1] for q in (.25, .5, .75)]
    if boundaries != prior.EXPECTED_YEAR_QUARTILE_BOUNDARIES:
        raise ValueError('Year boundaries require a design decision.')
    for row in pool:
        year = int(row['year']) if row['year'] is not None else None
        row['year_quartile'] = 'Y0_NULL' if year is None else 'Y1' if year <= boundaries[0] else 'Y2' if year <= boundaries[1] else 'Y3' if year <= boundaries[2] else 'Y4'
    return pool, grouped, {'authenticated_provenance': provenance, 'exclusion_checks': checks,
                           'party_available': dict(Counter(p['party'] for p in pool)),
                           'eligible_parent_count': len(pool), 'eligible_component_count': len(hashes),
                           'forbidden_sources': forbidden, 'current_cycles': current_cycles}


def build_selection():
    pool, grouped, metadata = build_pool()
    by_hash = defaultdict(list)
    for row in pool:
        by_hash[row['source_hash']].append(row)
    clusters = [by_hash[key] for key in sorted(by_hash)]
    party_targets = {key: min(limit, metadata['party_available'].get(key, 0)) for key, limit in prior.PARTY_LIMITS.items()}
    with fresh_seed():
        reserve, selected, proof = prior.canonical_sample_witness(clusters, {'RG': 0, 'RM': 0, 'RS': 0, 'RZ': 0}, prior.PARENT_TARGETS, prior.FLAG_MINIMA, party_targets)
        selected = prior._rebind_solver_components(selected, clusters, 'pilot')
        if reserve or proof.get('success') is not True or proof.get('mip_gap') != 0:
            raise ValueError('The original constraints lack an exact feasible assignment.')
        identity = prior._verify_assignment_proof(clusters, reserve, selected, proof)
        selected_rows, summary = prior._validate_witness(selected, metadata)
        components = sorted([{'component_rank': prior.canonical_component_rank(component), 'source_text_sha256': component[0]['source_hash'], 'parent_document_ids': sorted(row['id'] for row in component)} for component in selected], key=lambda c: c['component_rank'])
    if any(row['source_hash'] in metadata['forbidden_sources'] or row['candidate'] in metadata['current_cycles'] for row in selected_rows):
        raise ValueError('Selected sources overlap an exclusion.')
    selection = {'version': 'candidate_endorsement_independent40_selection_v1', 'sample_round': SAMPLE_ROUND,
                 'seed': SEED, 'selection_spec': SPEC, 'selection_spec_sha256': sha256_json(SPEC),
                 'selector_script_sha256': prior.sha256_file(Path(__file__)),
                 'pool_census_sha256': CENSUS_SHA256, 'authenticated_provenance': metadata['authenticated_provenance'],
                 'exclusion_checks': metadata['exclusion_checks'], 'pilot_components': components,
                 'pilot_parent_count': len(selected_rows), **identity}
    report = {'version': 'candidate_endorsement_independent40_selection_report_v1', 'feasible': True,
              'seed': SEED, 'sample_round': SAMPLE_ROUND, 'selection_spec_sha256': sha256_json(SPEC),
              'pool_census_sha256': CENSUS_SHA256, 'eligible_parent_count': len(pool),
              'eligible_component_count': len(clusters), **summary, 'solver_proof': proof,
              'exclusion_overlap_parents': 0, 'sealed_reserve_sources_reconstructed': False,
              'human_reviewed': False, 'ai_reviewed': False, 'production_authorized': False}
    # Selection must reach disk before the selected source task is constructed.
    return selection, report, grouped


def feasibility_check():
    pool, _, metadata = build_pool()
    by_hash = defaultdict(list)
    for row in pool:
        by_hash[row['source_hash']].append(row)
    party_targets = {key: min(limit, metadata['party_available'].get(key, 0)) for key, limit in prior.PARTY_LIMITS.items()}
    old = prior.base.SEED
    prior.base.SEED = SEED
    try:
        reserve, selected, proof = prior.base.exact_joint_witness(list(by_hash.values()), {'RG': 0, 'RM': 0, 'RS': 0, 'RZ': 0}, prior.PARENT_TARGETS, prior.FLAG_MINIMA, party_targets)
    finally:
        prior.base.SEED = old
    if reserve or proof.get('success') is not True or proof.get('mip_gap') != 0:
        raise ValueError('The original constraints lack an exact feasible assignment.')
    _, summary = prior._validate_witness(selected, metadata)
    return {'version': 'candidate_endorsement_independent40_feasibility_v1', 'feasible': True,
            'aggregate_only_no_parent_ids_source_hashes_or_text': True,
            'seed': SEED, 'selection_spec_sha256': sha256_json(SPEC),
            'eligible_parent_count': len(pool), 'eligible_component_count': len(by_hash),
            **summary, 'solver_status': proof['status'], 'mip_gap': proof['mip_gap'],
            'selection_frozen': False, 'model_calls': False, 'human_reviewed': False}


def write_json(path, value):
    data = (json.dumps(value, ensure_ascii=False, indent=2) + '\n').encode()
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists() and path.read_bytes() != data:
        raise ValueError(f'Refuse to overwrite different bytes: {path}')
    path.write_bytes(data)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output-dir', type=Path, default=OUTPUT)
    parser.add_argument('--feasibility-only', action='store_true')
    args = parser.parse_args()
    if args.feasibility_only:
        report = feasibility_check()
        write_json(args.output_dir / 'feasibility.json', report)
        print(json.dumps(report, indent=2))
        return 0
    selection, report, grouped = build_selection()
    write_json(args.output_dir / 'selection.json', selection)
    write_json(args.output_dir / 'report.json', report)
    # Only selected allowed components enter this source package.
    parents = []
    for component in selection['pilot_components']:
        for pid in component['parent_document_ids']:
            parent = reconstruct_parent_inventory(grouped[pid])
            if parent['extraction_source_text_sha256'] != component['source_text_sha256']:
                raise ValueError('Selected source differs from the frozen assignment.')
            parents.append(parent)
    write_json(args.output_dir / 'selected_sources.json', {
        'version': 'candidate_endorsement_independent40_selected_sources_v1',
        'selection_sha256': prior.sha256_file(args.output_dir / 'selection.json'),
        'parents': parents, 'human_reviewed': False, 'ai_reviewed': False,
        'production_authorized': False, 'model_calls': False,
    })
    print(json.dumps({'output_dir': str(args.output_dir.relative_to(REPO)), 'selection_sha256': prior.sha256_file(args.output_dir / 'selection.json'), **report}, indent=2))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
