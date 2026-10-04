import copy
import json

import pytest

from scripts import diagnose_candidate_endorsement_independent40 as d


def child(start, status='accepted_by_ai_review'):
    return {'entity_type': 'individual', 'name_spans': [[start, start + 3]],
            'support_spans': [[0, 11]], 'endorsement_status': status,
            'human_reviewed': False, 'human_confirmed': False}


def inputs():
    text = 'Ann Ann Bob'
    sha = d.digest(text.encode())
    metadata = dict(zip(d.METADATA_FIELDS, ['cycle', 1, 'CA', 'house', 2024, 'D']))
    sources, parents, responses = {}, [], []
    for index in range(1, 41):
        parent_id = f'p{index:02d}'
        sources[parent_id] = {'parent_index': index, 'parent_document_id': parent_id,
                              'source_text': text, 'source_text_sha256': sha,
                              'metadata': {**metadata, 'source_urls': ['https://example.test']}}
        predictions = [child(4), child(8)] if index == 1 else []
        blocks = [{'unresolved_block_id': 'failed-slot', 'block_span': [0, 11],
                   'human_reviewed': False, 'reason': 'model_failed'}] if index == 1 else []
        parents.append({'parent_document_id': parent_id, 'parent_source_text': text,
                        'source_text_sha256': sha, 'source_n_chars': len(text),
                        'metadata': metadata, 'human_parent_confirmed': False,
                        'ai_children': predictions, 'ai_unresolved_blocks': blocks})
        responses.append({'parent_document_id': parent_id,
                          'children': [child(0), child(4, 'uncertain')] if index == 1 else [],
                          'uncertainties': []})
    manifest = {'provenance': {'sample_round': 4, 'stage_a_manifest_sha256': 'a' * 64}}
    instructions = {'original_instructions': 'approved instructions', 'original_contract': {'rule': 'approved'}}
    task = {'parent_count': 40, 'parents': parents, 'authorizations': {'release': False},
            'instructions': instructions['original_instructions'], 'contract': instructions['original_contract'],
            'input_provenance': {'stage_a_input_manifest_sha256': 'a' * 64,
                                 'ai_child_count': 2, 'ai_unresolved_block_count': 1}}
    return task, {'responses': responses}, sources, manifest, instructions


def build(values):
    return d.build_diagnostic(*values, 'b' * 64)


def test_full_denominator_accepted_priority_and_failed_block_preservation():
    values = inputs()
    original = copy.deepcopy(values)
    report, ledger = build(values)
    exact = report['comparison']['exact_name_coordinates_and_type']
    normalized = report['comparison']['normalized_name_and_type_multiset']
    assert report['parent_count'] == len(ledger['parents']) == 40
    assert report['reference_is_ai_not_human_truth'] is True
    assert report['human_reviewed'] is False
    assert exact['prediction_matches_ai_accepted'] == 0
    assert exact['prediction_matches_ai_uncertain'] == 1
    assert normalized['prediction_matches_ai_accepted'] == 1
    assert normalized['prediction_matches_ai_uncertain'] == 0
    assert normalized['prediction_without_ai_reference'] == 1
    assert normalized['ai_uncertain_reference_not_matched'] == 1
    assert ledger['parents'][0]['reference_only'][0]['printed_name'] == 'Ann'
    assert ledger['parents'][0]['prediction_only'][0]['printed_name'] == 'Bob'
    assert ledger['parents'][0]['reference_only'][0]['overlapping_unresolved_extraction_block_ids'] == ['failed-slot']
    assert ledger['parents'][0]['unresolved_extraction_blocks'] == values[0]['parents'][0]['ai_unresolved_blocks']
    assert report['unresolved_extraction_block_count'] == 1
    assert values == original


def test_source_identity_mapping_does_not_depend_on_task_order():
    values = inputs()
    expected = build(values)
    values[0]['parents'].reverse()
    values[1]['responses'].reverse()
    assert build(values) == expected


@pytest.mark.parametrize('change', ['closure', 'same_length_wrong_id', 'duplicate_parent',
                                     'source_bytes', 'source_hash', 'source_length', 'metadata',
                                     'round', 'stage_a_pin', 'instructions', 'contract',
                                     'duplicate_prediction', 'count', 'failed_count',
                                     'duplicate_failed_block', 'bad_span', 'bad_block_span'])
def test_mismatches_fail(change):
    values = inputs()
    task, reference, sources, manifest, instructions = values
    parent = task['parents'][0]
    if change == 'closure':
        task['parents'].pop()
    elif change == 'same_length_wrong_id':
        parent['parent_document_id'] = 'other'
    elif change == 'duplicate_parent':
        task['parents'][1] = copy.deepcopy(parent)
    elif change == 'source_bytes':
        parent['parent_source_text'] += 'x'
    elif change == 'source_hash':
        parent['source_text_sha256'] = 'c' * 64
    elif change == 'source_length':
        parent['source_n_chars'] += 1
    elif change == 'metadata':
        parent['metadata'] = {**parent['metadata'], 'year': 2020}
    elif change == 'round':
        manifest['provenance']['sample_round'] = 3
    elif change == 'stage_a_pin':
        task['input_provenance']['stage_a_input_manifest_sha256'] = 'c' * 64
    elif change == 'instructions':
        task['instructions'] = 'changed'
    elif change == 'contract':
        task['contract'] = {}
    elif change == 'duplicate_prediction':
        parent['ai_children'].append(copy.deepcopy(parent['ai_children'][0]))
    elif change == 'count':
        task['input_provenance']['ai_child_count'] = 99
    elif change == 'failed_count':
        task['input_provenance']['ai_unresolved_block_count'] = 99
    elif change == 'duplicate_failed_block':
        parent['ai_unresolved_blocks'] *= 2
    elif change == 'bad_span':
        parent['ai_children'][0]['name_spans'] = [[0, 99]]
    else:
        parent['ai_unresolved_blocks'][0]['block_span'] = [0, 99]
    with pytest.raises(ValueError):
        build(values)


@pytest.mark.parametrize('level,field', [('task', 'human_reviewed'), ('parent', 'human_parent_confirmed'),
                                         ('child', 'human_confirmed'), ('child', 'confirmed'),
                                         ('block', 'human_reviewed')])
def test_confirmation_claims_fail(level, field):
    values = inputs()
    task = values[0]
    locations = {'task': task, 'parent': task['parents'][0],
                 'child': task['parents'][0]['ai_children'][0],
                 'block': task['parents'][0]['ai_unresolved_blocks'][0]}
    locations[level][field] = True
    with pytest.raises(ValueError, match='human confirmation'):
        build(values)


def test_empty_ai_support_is_allowed_without_human_confirmation():
    values = inputs()
    values[0]['parents'][0]['ai_children'][0]['support_spans'] = []
    assert build(values)[0]['parent_count'] == 40


def test_hash_pin_is_required_and_compares_raw_bytes(tmp_path):
    path = tmp_path / 'task.json'
    raw = json.dumps({'parents': []}).encode()
    path.write_bytes(raw)
    assert d.pinned_json(path, d.digest(raw)) == {'parents': []}
    for pin in (None, '', 'bad', 'a' * 64):
        with pytest.raises(ValueError):
            d.pinned_json(path, pin)
    path.write_bytes(raw + b'\n')
    with pytest.raises(ValueError):
        d.pinned_json(path, d.digest(raw))


def synthetic_reference(tmp_path, monkeypatch, change=None):
    _, _, sources, _, approved = inputs()

    def write(name, value):
        path = tmp_path / name
        path.parent.mkdir(parents=True, exist_ok=True)
        raw = (json.dumps(value) + '\n').encode()
        path.write_bytes(raw)
        return d.digest(raw)

    inventory, entries, responses = [], [], []
    for parent_id, source in sources.items():
        index = source['parent_index']
        source = {**source, 'human_reviewed': False}
        file = f'source/parent_{index:02d}.json'
        entries.append({'parent_index': index, 'parent_document_id': parent_id,
                        'source_text_sha256': source['source_text_sha256'],
                        'file': file, 'file_sha256': write(file, source)})
        review_child = child(0)
        review_child.pop('human_confirmed')
        review = {'parent_index': index, 'parent_document_id': parent_id,
                  'source_text_sha256': source['source_text_sha256'],
                  'review_status': 'ai_reviewed', 'human_reviewed': False,
                  'complete_source_reviewed': True,
                  'reviewer_id': 'independent_blind_01_20' if index <= 20 else 'independent_blind_21_40',
                  'reviewer_model': 'gpt-5.6-sol', 'children': [review_child],
                  'uncertainties': [], 'notes': '', 'authorizations': d.independent.AUTHORIZATIONS}
        if index == 1 and change == 'model':
            review['reviewer_model'] = 'other-model'
        if index == 1 and change == 'human':
            review_child['human_reviewed'] = True
        if index == 1 and change == 'review_span':
            review_child['name_spans'] = [[0, 999]]
        if index == 1 and change == 'uncertainty_span':
            review['uncertainties'] = [{'source_spans': [[0, 999]], 'reason': 'uncertain'}]
        file = f'blind/parent_{index:02d}.json'
        inventory.append({'parent_index': index, 'file': file, 'sha256': write(file, review)})
        responses.append({**review, 'metadata': source['metadata']})
    manifest = {'parent_count': 40, 'source_only_no_model_seeds': True,
                'human_reviewed': False, 'model_calls': False,
                'authorizations': d.independent.AUTHORIZATIONS,
                'provenance': {'sample_round': 4, 'stage_a_manifest_sha256': d.independent.STAGE_A_SHA256},
                'parents': entries}
    manifest_sha = write('manifest.json', manifest)
    instructions = {**approved, 'source_task_sha256': 'c' * 64, 'human_reviewed': False,
                    'authorizations': d.independent.AUTHORIZATIONS}
    instruction_sha = write('instructions.json', instructions)
    write('instruction_provenance.json', {
        'instructions_sha256': instruction_sha, 'source_task_sha256': 'c' * 64,
        'source_manifest_sha256': manifest_sha, 'human_reviewed': False,
        'authorizations': d.independent.AUTHORIZATIONS})
    reference = {'version': 'independent40_blind_ai_reference_v1',
                 'review_status': 'complete_blind_ai_review_not_human_truth',
                 'source_manifest_sha256': manifest_sha, 'instructions_sha256': instruction_sha,
                 'human_reviewed': False, 'authorizations': d.independent.AUTHORIZATIONS,
                 'responses': responses, 'review_inventory': inventory,
                 'counts': {'parents': 40, 'accepted_children': 40,
                            'uncertain_children': 0, 'uncertainty_blocks': 0, 'ai_zero_parents': 0}}
    if change == 'reference_count':
        reference['counts']['accepted_children'] = 41
    if change == 'reference_closure':
        reference['responses'].pop()
    monkeypatch.setattr(d, 'REFERENCE_SHA256', write('blind_reviewed_reference.json', reference))
    stage_sources = [{'parent_document_id': parent_id, 'parent_source_text': source['source_text'],
                      'extraction_source_text_sha256': source['source_text_sha256'],
                      **source['metadata']} for parent_id, source in sources.items()]
    monkeypatch.setattr(d.independent.app, 'load_stage_a_source_artifact',
                        lambda *args, **kwargs: (stage_sources, {'sample_round': 4}, d.independent.STAGE_A_SHA256))
    if change == 'source_file_hash':
        path = tmp_path / 'source/parent_01.json'
        path.write_bytes(path.read_bytes() + b'\n')
    if change == 'review_file_hash':
        path = tmp_path / 'blind/parent_01.json'
        path.write_bytes(path.read_bytes() + b'\n')
    if change == 'instruction_hash':
        path = tmp_path / 'instructions.json'
        path.write_bytes(path.read_bytes() + b'\n')


def test_authenticates_complete_synthetic_reference(tmp_path, monkeypatch):
    synthetic_reference(tmp_path, monkeypatch)
    reference, sources, _, _ = d.load_reference(tmp_path)
    assert len(sources) == 40
    assert reference['counts']['accepted_children'] == 40


@pytest.mark.parametrize('change', ['model', 'human', 'review_span', 'uncertainty_span',
                                     'reference_count', 'reference_closure',
                                     'source_file_hash', 'review_file_hash', 'instruction_hash'])
def test_reference_authentication_rejects_changes(tmp_path, monkeypatch, change):
    synthetic_reference(tmp_path, monkeypatch, change)
    with pytest.raises(ValueError):
        d.load_reference(tmp_path)


@pytest.mark.skipif(not (d.independent.OUTPUT / 'blind_reviewed_reference.json').is_file(),
                    reason='Private frozen independent reference is not available')
def test_standard_constructor_on_actual_authenticated_sources():
    reference, sources, manifest, instructions = d.load_reference()
    parents, stage_manifest, stage_hash = d.independent.app.load_stage_a_source_artifact(
        d.independent.adapter.OUTPUT, expected_manifest_sha256=d.independent.STAGE_A_SHA256)
    draft = {'parents': [{'parent_document_id': parent['parent_document_id'],
                          'provisional_parent_disposition': 'needs_review',
                          'children': [], 'unresolved_blocks': []} for parent in parents],
             'provenance': {'stage_a_output_inventory_sha256': 'b' * 64,
                            'stage_b_input_manifest_sha256': 'c' * 64,
                            'stage_b_output_inventory_sha256': 'd' * 64}}
    task = d.independent.app.build_task(
        parents=parents, stage_a_manifest=stage_manifest,
        stage_a_manifest_sha256=stage_hash, draft=draft, draft_sha256='e' * 64,
        task_id='independent40_constructor_compatibility_test')
    report, ledger = d.build_diagnostic(task, reference, sources, manifest, instructions, 'f' * 64)
    assert report['parent_count'] == len(ledger['parents']) == 40
    assert report['comparison']['exact_name_coordinates_and_type']['prediction_count'] == 0
    assert report['comparison']['exact_name_coordinates_and_type']['ai_accepted_reference_count'] == 2372
