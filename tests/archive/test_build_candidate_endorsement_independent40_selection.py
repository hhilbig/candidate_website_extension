from collections import Counter
import copy
import json

import pytest

from scripts import build_candidate_endorsement_independent40_selection as selector


def test_contract_reuses_constraints_and_uses_next_seed():
    assert selector.SEED == 20263827
    for key in ('exact_parent_quotas', 'cross_cutting_minima', 'party_minima_rule',
                'candidate_cycle_parent_cap', 'partition_unit', 'canonical_selector'):
        assert selector.SPEC[key] == selector.prior.SELECTION_SPEC[key]
    assert selector.SPEC['authorization']['hive_submission'] is False
    assert selector.SPEC['authorization']['human_truth'] is False


def test_seed_context_restores_historical_seed_after_failure():
    old = selector.prior.SEED
    with pytest.raises(RuntimeError), selector.fresh_seed():
        assert selector.prior.SEED == selector.SEED
        raise RuntimeError('test')
    assert selector.prior.SEED == old == 20262827


@pytest.mark.parametrize('key', ['extraction_source_text_sha256', 'candidate_cycle_id'])
def test_conflicting_source_identity_fails(key):
    row = {'extraction_source_text_sha256': 'a', 'candidate_cycle_id': 'b'}
    second = {**row, key: 'changed'}
    with pytest.raises(ValueError):
        selector.source_identity([row, second])


def test_writer_refuses_different_assignment(tmp_path):
    path = tmp_path / 'selection.json'
    selector.write_json(path, {'seed': selector.SEED})
    selector.write_json(path, {'seed': selector.SEED})
    with pytest.raises(ValueError):
        selector.write_json(path, {'seed': selector.SEED + 1})


def test_pool_authenticates_exclusions_before_source_reconstruction():
    seen = []
    def record(rows):
        source, cycle = selector.source_identity(rows)
        seen.append((source, cycle))
        return selector.reconstruct_parent_inventory(rows)
    pool, _, metadata = selector.build_pool(reconstruct=record)
    assert len(pool) == len(seen) == 1622
    assert len({source for source, _ in seen}) == 1579
    assert dict(Counter(row['pilot'] for row in pool)) == {'M': 1403, 'Z': 219}
    assert not {source for source, _ in seen} & metadata['forbidden_sources']
    assert not {cycle for _, cycle in seen} & metadata['current_cycles']


def test_generated_artifacts_match_frozen_assignment():
    if not (selector.OUTPUT / 'selected_sources.json').exists():
        pytest.skip('Run the selector before the artifact test.')
    selection = json.loads((selector.OUTPUT / 'selection.json').read_bytes())
    report = json.loads((selector.OUTPUT / 'report.json').read_bytes())
    package = json.loads((selector.OUTPUT / 'selected_sources.json').read_bytes())
    assert selection['seed'] == selector.SEED
    assert selection['selection_spec'] == selector.SPEC
    assert selection['selection_spec_sha256'] == selector.sha256_json(selector.SPEC)
    assert package['selection_sha256'] == selector.prior.sha256_file(selector.OUTPUT / 'selection.json')
    selected = {pid: component['source_text_sha256'] for component in selection['pilot_components'] for pid in component['parent_document_ids']}
    assert len(selected) == len(package['parents']) == 40
    assert {p['parent_document_id'] for p in package['parents']} == set(selected)
    assert all(p['extraction_source_text_sha256'] == selected[p['parent_document_id']] for p in package['parents'])
    assert report['primary'] == {'M': 32, 'Z': 8}
    assert report['candidate_cap'] <= 2
    assert report['solver_proof']['mip_gap'] == 0
    assert report['exclusion_overlap_parents'] == 0
    assert not package['model_calls'] and not package['human_reviewed'] and not package['ai_reviewed']
    assert all(report['flags'][flag] >= minimum for flag, minimum in selector.prior.FLAG_MINIMA.items())
