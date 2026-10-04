import copy
import gzip
import json
from argparse import Namespace

import pytest

from scripts import build_candidate_endorsement_independent40_stage_a as adapter


def pairs_and_ids():
    selection = adapter.selection_helper.prior.strict_json(adapter.selection_helper.OUTPUT / 'selection.json')
    pairs = {pid: component['source_text_sha256'] for component in selection['pilot_components'] for pid in component['parent_document_ids']}
    return pairs, sorted(pairs)


@pytest.mark.parametrize('sample_round', [False, 0, 1, 5, '4'])
def test_runner_rejects_unsupported_round(sample_round):
    with pytest.raises(ValueError):
        adapter.runner.validate_sample_round_binding(sample_round, {}, [])


def test_runner_rejects_unbound_sample_four():
    with pytest.raises(ValueError):
        adapter.runner.validate_sample_round_binding(4, {}, [])


@pytest.mark.parametrize('field', ['selection_sha256', 'selected_sources_sha256', 'selection_spec_sha256', 'seed'])
def test_runner_rejects_wrong_sample_four_binding(field):
    pairs, ids = pairs_and_ids()
    binding = {**adapter.runner.INDEPENDENT_SAMPLE4_BINDING, field: 'changed'}
    with pytest.raises(ValueError):
        adapter.runner.validate_sample_round_binding(4, {'independent_sample_binding': binding, 'independent_sample_source_pairs': pairs}, ids)


def test_runner_rejects_missing_parent_or_wrong_source():
    pairs, ids = pairs_and_ids()
    provenance = {'independent_sample_binding': adapter.runner.INDEPENDENT_SAMPLE4_BINDING, 'independent_sample_source_pairs': pairs}
    with pytest.raises(ValueError):
        adapter.runner.validate_sample_round_binding(4, provenance, ids[:-1])
    changed = {**pairs, ids[0]: '0' * 64}
    with pytest.raises(ValueError):
        adapter.runner.validate_sample_round_binding(4, {**provenance, 'independent_sample_source_pairs': changed}, ids)


def test_sample_four_binding_rejects_float_metadata():
    pairs, ids = pairs_and_ids()
    binding = {**adapter.runner.INDEPENDENT_SAMPLE4_BINDING, 'sample_round': 4.0}
    with pytest.raises(ValueError):
        adapter.runner.validate_sample_round_binding(4, {'independent_sample_binding': binding, 'independent_sample_source_pairs': pairs}, ids)


def test_manifest_rejects_batch_source_outside_frozen_selection(tmp_path):
    parents, provenance = adapter.authenticated_parents(adapter.selection_helper.OUTPUT / 'selection.json', adapter.selection_helper.OUTPUT / 'selected_sources.json')
    batch = adapter.builder.build_fresh_stage_a_batches(parents[:1])[0]
    batch['extraction_source_text_sha256'] = '0' * 64
    output = tmp_path / 'wrong-source'
    adapter.builder._write_artifact(output_dir=output, stage='line_decision', batches=[batch],
                                    selected_parent_ids=sorted(p['parent_document_id'] for p in parents),
                                    provenance=provenance, chunk_size=60, sample_round=4)
    with pytest.raises(ValueError, match='batch source'):
        adapter.runner.validate_input_manifest(output / 'manifest.json', output / 'chunks', stage='line_decision',
                                               expected_manifest_sha256=adapter.selection_helper.prior.sha256_file(output / 'manifest.json'))


def test_default_manifest_bytes_equal_explicit_historical_sample_three(tmp_path):
    parents = adapter.selection_helper.prior.strict_json(adapter.selection_helper.OUTPUT / 'selected_sources.json')['parents']
    batch = adapter.builder.build_fresh_stage_a_batches(parents[:1])[0]
    common = dict(stage='line_decision', batches=[batch], selected_parent_ids=[batch['parent_document_id']], provenance={}, chunk_size=60)
    adapter.builder._write_artifact(output_dir=tmp_path / 'default', **common)
    adapter.builder._write_artifact(output_dir=tmp_path / 'explicit', sample_round=3, **common)
    assert (tmp_path / 'default/manifest.json').read_bytes() == (tmp_path / 'explicit/manifest.json').read_bytes()
    assert (tmp_path / 'default/chunks/chunk_00000.jsonl.gz').read_bytes() == (tmp_path / 'explicit/chunks/chunk_00000.jsonl.gz').read_bytes()


def test_authenticated_adapter_rejects_changed_line_ledger(monkeypatch):
    original = adapter.selection_helper.reconstruct_parent_inventory
    def changed(rows):
        parent = original(rows)
        parent['lines'][0]['deterministic_disposition'] = 'changed'
        return parent
    monkeypatch.setattr(adapter.selection_helper, 'reconstruct_parent_inventory', changed)
    with pytest.raises(ValueError, match='line ledger'):
        adapter.authenticated_parents(adapter.selection_helper.OUTPUT / 'selection.json', adapter.selection_helper.OUTPUT / 'selected_sources.json')


def test_independent_artifact_passes_full_manifest_validation(tmp_path):
    manifest, digest = adapter.build_artifact(tmp_path / 'independent')
    observed, observed_digest, paths = adapter.runner.validate_input_manifest(tmp_path / 'independent/manifest.json', tmp_path / 'independent/chunks', stage='line_decision', expected_manifest_sha256=digest)
    assert observed == manifest and observed_digest == digest
    assert manifest['sample_round'] == 4 and manifest['selected_parent_count'] == 40
    assert manifest['provenance']['independent_sample_binding'] == adapter.runner.INDEPENDENT_SAMPLE4_BINDING
    assert len(paths) == len(manifest['chunks'])
    rows = [json.loads(line) for path in paths for line in gzip.decompress(path.read_bytes()).decode().splitlines()]
    assert len(rows) == manifest['batch_count']
    assert set(row['parent_document_id'] for row in rows) <= set(manifest['selected_parent_ids'])
    changed = copy.deepcopy(manifest)
    changed['provenance'].pop('independent_sample_binding')
    changed.pop('manifest_payload_sha256')
    changed['manifest_payload_sha256'] = adapter.builder.sha256_json(changed)
    path = tmp_path / 'independent/manifest.json'
    path.write_text(adapter.builder.canonical_json(changed) + '\n')
    with pytest.raises(ValueError, match='binding'):
        adapter.runner.validate_input_manifest(path, path.parent / 'chunks', stage='line_decision', expected_manifest_sha256=adapter.selection_helper.prior.sha256_file(path))


def test_actual_adapter_artifact_loads_all_sources_in_review_app(tmp_path):
    from scripts import build_candidate_endorsement_round4_human_review_app as app
    output = tmp_path / 'adapter-stage-a'
    manifest, digest = adapter.build_artifact(output)
    expected_fields = set(app.SOURCE_PROVENANCE_FIELDS) | {'independent_sample_binding', 'independent_sample_source_pairs'}
    assert set(manifest['provenance']) == expected_fields
    parents, observed_manifest, observed_digest = app.load_stage_a_source_artifact(output, expected_manifest_sha256=digest)
    assert observed_manifest == manifest and observed_digest == digest
    package = adapter.selection_helper.prior.strict_json(adapter.selection_helper.OUTPUT / 'selected_sources.json')
    expected = {parent['parent_document_id']: parent['parent_source_text'] for parent in package['parents']}
    assert len(parents) == 40
    assert {parent['parent_document_id']: parent['parent_source_text'] for parent in parents} == expected


def test_synthetic_stage_a_to_b_preserves_independent_binding(tmp_path):
    input_dir = tmp_path / 'stage-a-input'
    manifest, digest = adapter.build_artifact(input_dir)
    args = Namespace(stage='line_decision', seed=adapter.runner.GENERATION_SEED,
                     max_model_len=adapter.runner.MAX_MODEL_LEN,
                     max_output_tokens=adapter.runner.MAX_OUTPUT_TOKENS['line_decision'],
                     gpu_memory_utilization=adapter.runner.GPU_MEMORY_UTILIZATION)
    generation = adapter.runner.generation_config_sha256(args, input_manifest_sha256=digest)
    paths = [input_dir / 'chunks' / chunk['file'] for chunk in manifest['chunks']]
    output_dir = tmp_path / 'synthetic-stage-a-output'
    output_dir.mkdir()
    all_rows = []
    for path in paths:
        rows = []
        for batch in adapter.runner.read_gzip_jsonl(path):
            model = {slot: {'role': 'record_anchor' if index == 0 else 'nonrecord_content'} for index, slot in enumerate(batch['target_slots'])}
            model.update({slot: {'role': 'padding'} for slot in batch['padding_slots']})
            rows.append(adapter.runner.derive_output_row(
                batch=batch, raw_output=adapter.builder.canonical_json(model), finish_reason='stop',
                stop_reason=None, output_tokens=100, prompt_tokens=500, args=args,
                input_manifest_sha256=digest,
                input_chunk_sha256=adapter.selection_helper.prior.sha256_file(path),
                generation_config_sha256_value=generation))
        (output_dir / path.name.replace('.jsonl.gz', '.out.jsonl.gz')).write_bytes(adapter.runner._gzip_jsonl_bytes(rows))
        all_rows.extend(rows)
    metadata = adapter.runner.run_metadata(args=args, input_manifest_sha256=digest,
                                          generation_config_sha256_value=generation,
                                          selected_chunks=paths, rows=all_rows,
                                          started_at='2026-09-14T00:00:00+00:00')
    (output_dir / 'run_metadata.json').write_text(json.dumps(metadata) + '\n')
    stage_b = tmp_path / 'stage-b-input'
    result = adapter.builder.build_fresh_stage_b_artifact(input_dir, output_dir, stage_b,
                                                        expected_stage_a_input_manifest_sha256=digest)
    assert result['sample_round'] == 4
    for key in ('independent_sample_binding', 'independent_sample_source_pairs'):
        assert result['provenance'][key] == manifest['provenance'][key]
    adapter.runner.validate_input_manifest(stage_b / 'manifest.json', stage_b / 'chunks', stage='record_extraction',
                                           expected_manifest_sha256=adapter.selection_helper.prior.sha256_file(stage_b / 'manifest.json'))
