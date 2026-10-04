import copy
import json

import pytest

from scripts import compare_candidate_endorsement_independent40_codex as comparison

review = comparison.review


@pytest.fixture
def package(tmp_path, monkeypatch):
    text = 'Alice supports Bob. Alice supports Bob.\n'
    parents = [{'parent_document_id': f'parent-{index:02d}', 'parent_source_text': text,
                'extraction_source_text_sha256': comparison.digest(text.encode()),
                **{key: 'test' for key in ('candidate_cycle_id', 'candidate_icpsr', 'state', 'office', 'year', 'party', 'source_urls')}} for index in range(1, 41)]
    monkeypatch.setattr(review, 'authority', lambda: (parents, {'test': True}))
    source = tmp_path / 'source-package'
    review.prepare(source)
    for index in range(1, 41):
        for pass_name in ('blind', 'seed_audit'):
            reviewer = f'independent_{pass_name}_' + ('01_20' if index <= 20 else '21_40')
            review.write_review(index, [], reviewer, pass_name=pass_name,
                                complete_source_reviewed=True, reviewer_model='gpt-5.6-sol', output_dir=source)
    review.freeze_blind_reference(source)
    return source, tmp_path / 'comparison'


def write_value(path, value):
    path.write_text(json.dumps(value))


def compare(package):
    source, output = package
    return comparison.compare(source, output, blind_sha256=comparison.digest((source / 'blind_reviewed_reference.json').read_bytes()))


def test_complete_comparison_preserves_initial_artifacts_and_freezes_seed(package):
    source, output = package
    before = {path.relative_to(source): path.read_bytes() for path in source.rglob('*.json')}
    result = compare(package)
    assert result['counts']['exact_identity_agreements'] == 0
    assert result['counts']['parents_requiring_adjudication'] == 0
    seed = json.loads((output / 'seed_audit_reviewed_reference.json').read_bytes())
    assert seed['counts']['ai_zero_parents'] == 40
    assert {value['reviewer_model'] for value in seed['responses']} == {'gpt-5.6-sol'}
    assert seed['human_reviewed'] is False and not any(seed['authorizations'].values())
    assert result['provenance']['seed_audit_reference_sha256'] == comparison.digest((output / 'seed_audit_reviewed_reference.json').read_bytes())
    assert before == {path.relative_to(source): path.read_bytes() for path in source.rglob('*.json')}
    with pytest.raises(ValueError, match='new directory'):
        compare(package)


def test_occurrence_status_uncertainty_and_disposition_are_compared(package):
    source, output = package
    parent = review.read_parent(1, source)
    row = review.child(parent, 'individual', 1, 'Alice')
    row['endorsement_status'] = 'accepted_by_ai_review'
    blind_path = source / 'blind/parent_01.json'
    a = json.loads(blind_path.read_bytes())
    a['children'] = [row]
    write_value(blind_path, a)
    (source / 'blind_reviewed_reference.json').unlink()  # Replace only this synthetic fixture's initial freeze.
    review.freeze_blind_reference(source)
    b = json.loads((source / 'seed_audit/parent_01.json').read_bytes())
    changed = copy.deepcopy(row)
    changed['endorsement_status'] = 'uncertain'
    other = copy.deepcopy(changed)
    other['name_spans'] = [review.name_span(parent, 1, 'Alice', 1)]
    b['children'] = [changed, other]
    b['uncertainties'] = [{'source_spans': [[0, 5]], 'reason': 'Test uncertainty.'}]
    write_value(source / 'seed_audit/parent_01.json', b)
    result = compare(package)
    counts = result['counts']
    assert counts['exact_identity_agreements'] == 1
    assert counts['exact_identity_status_agreements'] == 0
    assert counts['status_disagreements'] == counts['seed_audit_only_identities'] == 1
    assert counts['parent_uncertainty_disagreements'] == counts['parent_disposition_disagreements'] == 1
    evidence = json.loads((output / 'private_adjudication_evidence.json').read_bytes())['parents'][0]
    assert evidence['blind_disposition'] == 'contains_children'
    assert evidence['seed_audit_disposition'] == 'unclear'
    assert evidence['seed_audit_only'][0]['name_evidence'][0]['source_text'] == 'Alice'
    assert evidence['seed_audit_uncertainties'][0]['evidence'][0]['source_text'] == 'Alice'
    assert evidence['status_disagreements'][0]['blind']['endorsement_status'] == 'accepted_by_ai_review'
    assert evidence['status_disagreements'][0]['seed_audit']['endorsement_status'] == 'uncertain'


def test_shared_uncertainty_requires_adjudication_without_claiming_disagreement(package):
    source, output = package
    for pass_name in ('blind', 'seed_audit'):
        path = source / pass_name / 'parent_01.json'
        value = json.loads(path.read_bytes())
        value['uncertainties'] = [{'source_spans': [[0, 5]], 'reason': 'Identical unresolved test.'}]
        write_value(path, value)
    (source / 'blind_reviewed_reference.json').unlink()  # Replace only this synthetic fixture's initial freeze.
    review.freeze_blind_reference(source)
    counts = compare(package)['counts']
    assert counts['parents_with_disagreements'] == 0
    assert counts['parents_with_retained_uncertainty'] == counts['parents_requiring_adjudication'] == 1
    assert json.loads((output / 'private_adjudication_evidence.json').read_bytes())['parent_count'] == 1


@pytest.mark.parametrize('change', ['missing', 'extra', 'model', 'reviewer', 'coverage', 'human', 'source', 'uncertainty_human', 'symlink'])
def test_seed_rejections_produce_no_output(package, change):
    source, output = package
    path = source / 'seed_audit/parent_40.json'
    value = json.loads(path.read_bytes())
    if change == 'missing':
        path.unlink()
    elif change == 'extra':
        write_value(source / 'seed_audit/extra.json', {})
    elif change == 'symlink':
        target = source / 'saved-parent.json'
        write_value(target, value)
        path.unlink()
        path.symlink_to(target)
    else:
        if change == 'model':
            value['reviewer_model'] = 'other-model'
        elif change == 'reviewer':
            value['reviewer_id'] = 'independent_seed_audit_01_20'
        elif change == 'coverage':
            value['complete_source_reviewed'] = False
        elif change == 'human':
            value['human_reviewed'] = True
        elif change == 'source':
            value['source_text_sha256'] = '0' * 64
        elif change == 'uncertainty_human':
            value['uncertainties'] = [{'source_spans': [[0, 5]], 'reason': 'Test.', 'human_confirmed': True}]
        write_value(path, value)
    with pytest.raises(ValueError):
        compare(package)
    assert not output.exists()


def test_pinned_blind_hash_and_frozen_inventory_are_enforced(package):
    source, output = package
    with pytest.raises(ValueError, match='reference hash'):
        comparison.compare(source, output, blind_sha256='0' * 64)
    path = source / 'blind/parent_40.json'
    value = json.loads(path.read_bytes())
    value['notes'] = 'Changed after freeze.'
    write_value(path, value)
    with pytest.raises(ValueError, match='frozen blind reference'):
        compare(package)
    assert not output.exists()


def test_prepared_source_authentication_is_not_bypassed(package):
    source, output = package
    path = source / 'source/parent_40.json'
    value = json.loads(path.read_bytes())
    value['source_text'] = 'Changed source.'
    write_value(path, value)
    with pytest.raises(ValueError, match='source file hash'):
        compare(package)
    assert not output.exists()
