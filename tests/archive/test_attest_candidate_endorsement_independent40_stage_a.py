import json
import sys

import pytest

from scripts import attest_candidate_endorsement_independent40_stage_a as attest
from scripts import build_candidate_endorsement_round4_development_inputs as builder


@pytest.fixture
def origin(monkeypatch, tmp_path):
    output_dir = tmp_path / 'output_stage_a'
    output_dir.mkdir()
    metadata = output_dir / 'run_metadata.json'
    metadata.write_text(json.dumps({'slurm_job_id': '23312106'}))
    rows = [{'technical_pass': True} for _ in range(291)] + [{'technical_pass': False}]
    authenticated = {'manifest': {'sample_round': 4}, 'parents': [{} for _ in range(40)]}
    monkeypatch.setattr(builder, 'authenticate_fresh_stage_a_run', lambda *a, **k: (
        [], rows, authenticated, {'output_inventory_sha256': 'a' * 64}))
    output = tmp_path / 'origin.json'
    monkeypatch.setattr(sys, 'argv', ['attest', '--project-root', str(tmp_path),
                                    '--output', str(output)])
    return rows, authenticated, metadata, output


def test_complete_inventory_preserves_failure(origin):
    _, _, _, output = origin
    assert attest.main() == 0
    result = json.loads(output.read_bytes())
    assert result['processed_count'] == 292
    assert result['technical_failure_count'] == 1
    assert result['complete_run_inventory'] is True
    assert result['human_reviewed'] is False
    assert attest.main() == 0


def test_incomplete_inventory_rejects(origin):
    rows, _, _, output = origin
    rows.pop()
    with pytest.raises(ValueError, match='complete frozen sample'):
        attest.main()
    assert not output.exists()


def test_different_sample_rejects(origin):
    _, authenticated, _, output = origin
    authenticated['manifest']['sample_round'] = 3
    with pytest.raises(ValueError, match='independent sample'):
        attest.main()
    assert not output.exists()


def test_different_job_rejects(origin):
    _, _, metadata, output = origin
    metadata.write_text(json.dumps({'slurm_job_id': 'other'}))
    with pytest.raises(ValueError, match='different Slurm job'):
        attest.main()
    assert not output.exists()


def test_different_result_cannot_overwrite(origin):
    rows, _, _, output = origin
    attest.main()
    original = output.read_bytes()
    rows[0]['technical_pass'] = False
    with pytest.raises(ValueError, match='overwrite'):
        attest.main()
    assert output.read_bytes() == original
