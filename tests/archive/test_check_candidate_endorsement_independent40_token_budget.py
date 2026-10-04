import json
import sys
from types import SimpleNamespace

import pytest

from scripts import check_candidate_endorsement_independent40_token_budget as check
from scripts.hive import extract_candidate_endorsement_round4_development_vllm as runner


@pytest.fixture
def budget_check(monkeypatch, tmp_path):
    state = {'counts': [100, 300], 'batch_count': 2, 'sample_round': 4}
    calls = []
    def tokenizer(path, **kwargs):
        calls.append((path, kwargs))
        return object()
    monkeypatch.setitem(sys.modules, 'transformers', SimpleNamespace(
        AutoTokenizer=SimpleNamespace(from_pretrained=tokenizer)))
    monkeypatch.setattr(runner, 'validate_input_manifest', lambda *a, **k: (
        {'sample_round': state['sample_round'], 'batch_count': state['batch_count']},
        'a' * 64, ['chunk']))
    monkeypatch.setattr(runner, 'read_gzip_jsonl', lambda path: state['counts'])
    monkeypatch.setattr(runner, 'build_conversation', lambda row: row)
    monkeypatch.setattr(runner, 'prompt_token_count', lambda tokenizer, row: row)
    monkeypatch.setattr(runner, 'MAX_MODEL_LEN', 1000)
    monkeypatch.setattr(runner, 'MAX_OUTPUT_TOKENS', {'line_decision': 100})
    output = tmp_path / 'budget.json'
    monkeypatch.setattr(sys, 'argv', ['check', '--project-root', str(tmp_path),
        '--input-dir', str(tmp_path), '--expected-manifest-sha256', 'a' * 64,
        '--model-path', str(tmp_path / runner.MODEL_REVISION), '--output', str(output)])
    return state, calls, output


def test_exact_budget_and_offline_tokenizer(budget_check):
    _, calls, output = budget_check
    assert check.main() == 0
    result = json.loads(output.read_bytes())
    assert result['minimum_context_margin_tokens'] == 600
    assert result['batch_count'] == 2 and result['budget_pass'] is True
    assert result['human_reviewed'] is False and result['inference_submitted'] is False
    assert calls[0][1] == {'local_files_only': True}
    assert check.main() == 0


def test_failed_budget_retains_result(budget_check):
    state, _, output = budget_check
    state['counts'] = [900, 900]
    assert check.main() == 2
    assert json.loads(output.read_bytes())['budget_pass'] is False


def test_count_mismatch_rejects(budget_check):
    state, _, output = budget_check
    state['batch_count'] = 3
    with pytest.raises(ValueError, match='prompt count'):
        check.main()
    assert not output.exists()


def test_nonindependent_sample_rejects(budget_check):
    state, _, output = budget_check
    state['sample_round'] = 3
    with pytest.raises(ValueError, match='independent sample'):
        check.main()
    assert not output.exists()


def test_different_result_never_overwrites(budget_check):
    state, _, output = budget_check
    assert check.main() == 0
    original = output.read_bytes()
    state['counts'] = [100, 400]
    with pytest.raises(ValueError, match='overwrite'):
        check.main()
    assert output.read_bytes() == original


def test_stage_b_uses_its_output_reserve(budget_check, monkeypatch):
    _, _, output = budget_check
    monkeypatch.setattr(sys, 'argv', sys.argv + ['--stage', 'record_extraction'])
    monkeypatch.setattr(runner, 'MAX_OUTPUT_TOKENS', {'record_extraction': 500})
    assert check.main() == 2
    assert json.loads(output.read_bytes())['minimum_context_margin_tokens'] == 200


def test_empty_input_rejects(budget_check):
    state, _, output = budget_check
    state.update(counts=[], batch_count=0)
    with pytest.raises(ValueError, match='no prompts'):
        check.main()
    assert not output.exists()
