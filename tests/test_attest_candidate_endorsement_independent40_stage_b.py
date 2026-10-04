import json
import sys

import pytest

from scripts import attest_candidate_endorsement_independent40_stage_b as attest
from scripts import build_candidate_endorsement_round4_development_inputs as builder
from scripts import build_candidate_endorsement_round4_human_review_app as app


@pytest.fixture
def origin(monkeypatch, tmp_path):
    (tmp_path / 'output_stage_b').mkdir()
    metadata = tmp_path / 'output_stage_b' / 'run_metadata.json'
    metadata.write_text(json.dumps({'slurm_job_id': '23312502'}))
    a_rows = [{'technical_pass': True} for _ in range(292)]
    a = {'manifest': {'sample_round': 4, 'selected_parent_ids': ['parent']},
         'parents': [{} for _ in range(40)]}
    rows = [{'technical_pass': True} for _ in range(638)] + [{'technical_pass': False}]
    b = {'sample_round': 4}
    calls = []
    monkeypatch.setattr(builder, 'authenticate_fresh_stage_a_run', lambda *a_args, **kw: (
        [], a_rows, a, {'output_inventory_sha256': 'a' * 64}))
    def stage_b(*a_args, **kwargs):
        calls.append(kwargs)
        return [], rows, b, {'output_inventory_sha256': 'b' * 64}
    monkeypatch.setattr(app, 'authenticate_fresh_stage_b_run', stage_b)
    output = tmp_path / 'origin.json'
    monkeypatch.setattr(sys, 'argv', ['attest', '--project-root', str(tmp_path),
                                    '--output', str(output)])
    return a_rows, a, rows, b, metadata, output, calls


def test_complete_inventory_preserves_failure_and_round_binding(origin):
    *_, output, calls = origin
    assert attest.main() == 0
    result = json.loads(output.read_bytes())
    assert result['processed_count'] == 639
    assert result['technical_failure_count'] == 1
    assert result['human_reviewed'] is False
    assert calls[0]['stage_a_sample_round'] == 4
    assert calls[0]['stage_a_output_provenance'] == {'output_inventory_sha256': 'a' * 64}
    assert attest.main() == 0


@pytest.mark.parametrize('position', [0, 2])
def test_incomplete_stage_rejects(origin, position):
    origin[position].pop()
    with pytest.raises(ValueError, match='complete'):
        attest.main()
    assert not origin[5].exists()


@pytest.mark.parametrize('position', [1, 3])
def test_wrong_sample_rejects(origin, position):
    value = origin[position]
    (value['manifest'] if position == 1 else value)['sample_round'] = 3
    with pytest.raises(ValueError, match='sample'):
        attest.main()
    assert not origin[5].exists()


def test_wrong_job_rejects(origin):
    origin[4].write_text(json.dumps({'slurm_job_id': 'other'}))
    with pytest.raises(ValueError, match='different Slurm job'):
        attest.main()
    assert not origin[5].exists()


def test_different_result_never_overwrites(origin):
    attest.main()
    original = origin[5].read_bytes()
    origin[2][0]['technical_pass'] = False
    with pytest.raises(ValueError, match='overwrite'):
        attest.main()
    assert origin[5].read_bytes() == original
