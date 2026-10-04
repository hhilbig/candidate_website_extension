import copy
import json

import pytest

from scripts import review_candidate_endorsement_independent40_codex as review


@pytest.fixture
def package(tmp_path, monkeypatch):
    parents = [{'parent_document_id': f'parent-{index:02d}', 'parent_source_text': 'Alice supports Bob.\n',
                'extraction_source_text_sha256': review.utilities.digest(b'Alice supports Bob.\n'),
                **{key: 'test' for key in ('candidate_cycle_id', 'candidate_icpsr', 'state', 'office', 'year', 'party', 'source_urls')}} for index in range(1, 41)]
    monkeypatch.setattr(review, 'authority', lambda: (parents, {'test': True}))
    review.prepare(tmp_path)
    return tmp_path


def test_prepared_source_contains_no_seeds_or_labels(package):
    manifest = json.loads((package / 'manifest.json').read_bytes())
    assert len(manifest['parents']) == 40
    parent = review.read_parent(1, package)
    assert 'ai_children' not in parent and 'children' not in parent
    assert parent['lines'] == review.source_lines(parent['source_text'])
    template = json.loads((package / 'review_template.json').read_bytes())
    assert all(not p['parent_confirmed'] and not p['ai_reviewed'] and p['parent_disposition'] == 'needs_review' for p in template['parents'])


def test_explicit_review_status_and_coverage_required(package):
    parent = review.read_parent(1, package)
    child = review.child(parent, 'individual', 1, 'Alice')
    with pytest.raises(ValueError, match='explicit AI'):
        review.write_review(1, [child], 'test', pass_name='blind', complete_source_reviewed=True, output_dir=package)
    child['endorsement_status'] = 'uncertain'
    with pytest.raises(ValueError, match='coverage'):
        review.write_review(1, [child], 'test', pass_name='blind', complete_source_reviewed=False, output_dir=package)
    result = review.write_review(1, [child], 'test', pass_name='blind', complete_source_reviewed=True,
                                 uncertainties=[{'source_spans': [[0, 5]], 'reason': 'Synthetic test uncertainty.'}], output_dir=package)
    assert result['children'][0]['endorsement_status'] == 'uncertain'
    assert result['human_reviewed'] is False


@pytest.mark.parametrize('change', ['text', 'metadata', 'lines', 'human'])
def test_rehashed_changed_source_still_fails_authoritative_comparison(package, change):
    path = package / 'source/parent_02.json'
    value = json.loads(path.read_bytes())
    if change == 'text':
        value['source_text'] = 'Changed text.'
    elif change == 'metadata':
        value['metadata']['state'] = 'changed'
    elif change == 'lines':
        value['lines'][0]['start'] = 1
    else:
        value['human_reviewed'] = True
    path.write_text(json.dumps(value))
    manifest_path = package / 'manifest.json'
    manifest = json.loads(manifest_path.read_bytes())
    manifest['parents'][1]['file_sha256'] = review.utilities.digest(path.read_bytes())
    manifest_path.write_text(json.dumps(manifest))
    receipt = json.loads((package / 'instruction_provenance.json').read_bytes())
    receipt['source_manifest_sha256'] = review.utilities.digest(manifest_path.read_bytes())
    (package / 'instruction_provenance.json').write_text(json.dumps(receipt))
    with pytest.raises(ValueError, match='identity or bytes'):
        review.read_parent(1, package)


def test_source_closure_hash_and_authority_rejections(package):
    path = package / 'source/parent_40.json'
    path.write_text('changed')
    with pytest.raises(ValueError, match='file hash'):
        review.read_parent(1, package)
    manifest_path = package / 'manifest.json'
    manifest = json.loads(manifest_path.read_bytes())
    manifest['authorizations']['human_truth'] = True
    manifest_path.write_text(json.dumps(manifest))
    receipt = json.loads((package / 'instruction_provenance.json').read_bytes())
    receipt['source_manifest_sha256'] = review.utilities.digest(manifest_path.read_bytes())
    (package / 'instruction_provenance.json').write_text(json.dumps(receipt))
    with pytest.raises(ValueError, match='authority'):
        review.read_parent(1, package)


def test_review_cannot_claim_human_truth_or_invalid_uncertainty(package):
    parent = review.read_parent(1, package)
    value = review.write_review(1, [], 'test', pass_name='blind', complete_source_reviewed=True, output_dir=package)
    bad = copy.deepcopy(value)
    bad['authorizations']['human_truth'] = True
    with pytest.raises(ValueError, match='authority'):
        review.validate_review(bad, parent)
    bad = copy.deepcopy(value)
    bad['uncertainties'] = [{'source_spans': [[0, 999]], 'reason': 'test'}]
    with pytest.raises(ValueError, match='uncertainty'):
        review.validate_review(bad, parent)


def test_original_module_globals_remain_unchanged():
    assert review.utilities.TASK_SHA256 == 'c92cb24a4c692f839f009b40f4be898e729db669da4c2e11e25fdec53fd195ea'
    assert review.utilities.OUTPUT.name == 'private_candidate_endorsement_codex_ai_review_20260914a'


def test_changed_instructions_fail(package):
    path = package / 'instructions.json'
    value = json.loads(path.read_bytes())
    value['source_task_sha256'] = '0' * 64
    path.write_text(json.dumps(value))
    with pytest.raises(ValueError, match='coding reference'):
        review.read_parent(1, package)


def test_unpinned_actual_manifest_fails(monkeypatch):
    monkeypatch.setattr(review, 'STAGE_A_SHA256', '0' * 64)
    with pytest.raises(ValueError):
        review.authority()


def test_actual_pinned_sample_package_loads_all_sources():
    manifest = json.loads((review.OUTPUT / 'manifest.json').read_bytes())
    parent = review.read_parent(40)
    assert len(manifest['parents']) == 40
    assert parent['parent_index'] == 40
    assert manifest['provenance']['stage_a_manifest_sha256'] == review.STAGE_A_SHA256
    assert manifest['source_only_no_model_seeds'] is True


@pytest.mark.parametrize('flag', ['human_reviewed', 'human_confirmed', 'confirmed'])
def test_child_confirmation_cannot_enter_ai_reference(package, flag):
    parent = review.read_parent(1, package)
    row = review.child(parent, 'individual', 1, 'Alice')
    row.update(endorsement_status='accepted_by_ai_review')
    row[flag] = True
    with pytest.raises(ValueError, match='human review or confirmation'):
        review.write_review(1, [row], 'test', pass_name='blind', complete_source_reviewed=True, output_dir=package)


def test_explicit_reviewer_model_is_preserved_and_validated(package):
    parent = review.read_parent(1, package)
    value = review.write_review(1, [], 'test', pass_name='blind', complete_source_reviewed=True,
                               reviewer_model='gpt-5.6-sol', output_dir=package)
    assert value['reviewer_model'] == 'gpt-5.6-sol'
    value['reviewer_model'] = ''
    with pytest.raises(ValueError, match='model provenance'):
        review.validate_review(value, parent)


def test_blind_freeze_requires_complete_inventory_and_expected_reviewers(package):
    (package / 'blind').mkdir()
    with pytest.raises(ValueError, match='all 40'):
        review.freeze_blind_reference(package)
    for index in range(1, 41):
        reviewer = 'independent_blind_01_20' if index <= 20 else 'independent_blind_21_40'
        review.write_review(index, [], reviewer, pass_name='blind', complete_source_reviewed=True,
                           reviewer_model='gpt-5.6-sol', output_dir=package)
    result = review.freeze_blind_reference(package)
    assert result['counts']['ai_zero_parents'] == 40
    assert result['human_reviewed'] is False and len(result['review_inventory']) == 40
    assert review.freeze_blind_reference(package) == result
    path = package / 'blind/parent_40.json'
    value = json.loads(path.read_bytes())
    value['reviewer_id'] = 'wrong'
    path.write_text(json.dumps(value))
    with pytest.raises(ValueError, match='reviewer provenance'):
        review.freeze_blind_reference(package)
