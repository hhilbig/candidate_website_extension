from __future__ import annotations

import copy
import gzip
import json
import subprocess
import sys
import types
from pathlib import Path
from types import SimpleNamespace

import pytest

from scripts.build_candidate_endorsement_round4_development_inputs import (
    _write_artifact,
)
from scripts.hive import extract_candidate_endorsement_round4_development_vllm as runner


REAL_FRESH_STAGE_A = (
    runner.REPO
    / "build/hive_candidate_extraction/"
    "v2_1_endorsement_recall40_sample_round3_round4_stage_a_20260907a"
)
PRIOR_SAFE_USER_MESSAGE_MAX_BYTES = 43_449


def _line(index: int, text: str) -> dict:
    return {
        "source_line_id": f"source-line-{index}",
        "parent_line_index": index,
        "parent_start_char": 0,
        "parent_end_char": len(text),
        "parent_full_end_char": len(text) + 1,
        "line_text": text,
        "line_text_sha256": runner.sha256_text(text),
        "line_terminator": "\n",
        "model_decision_required": True,
        "deterministic_disposition": "model_decision_required",
    }


def _stage_a_batch() -> dict:
    line = _line(0, "One Person")
    batch_id = "a" * 64
    return {
        "version": runner.MANIFEST_VERSION,
        "stage": runner.LINE_DECISION_STAGE,
        "batch_id": batch_id,
        "document_id": batch_id,
        "parent_document_id": "parent-a",
        "candidate_cycle_id": "candidate-a",
        "target_slots": ["slot_00"],
        "padding_slots": [
            f"slot_{index:02d}"
            for index in range(1, runner.STAGE_A_SLOT_COUNT)
        ],
        "targets": [{"slot": "slot_00", **copy.deepcopy(line)}],
        "visible_lines": [copy.deepcopy(line)],
        "full_parent_line_catalog_sha256": runner.sha256_json([line]),
    }


def _stage_b_batch() -> dict:
    line = _line(0, "Alex and Jamie Rivera, Teachers Association")
    batch_id = "b" * 64
    return {
        "version": runner.MANIFEST_VERSION,
        "stage": runner.RECORD_EXTRACTION_STAGE,
        "batch_id": batch_id,
        "document_id": batch_id,
        "source_round3_batch_id": "c" * 64,
        "parent_document_id": "parent-b",
        "candidate_cycle_id": "candidate-b",
        "target_slots": ["slot_00"],
        "padding_slots": ["slot_01", "slot_02", "slot_03"],
        "targets": [
            {
                "slot": "slot_00",
                "source_line_id": line["source_line_id"],
                "parent_line_index": line["parent_line_index"],
                "parent_start_char": line["parent_start_char"],
                "parent_end_char": line["parent_end_char"],
                "line_text": line["line_text"],
                "line_text_sha256": line["line_text_sha256"],
                "stage_a_role": "compound_record_anchor",
                "record_block_lines": [copy.deepcopy(line)],
            }
        ],
    }


def _padding_decision() -> dict:
    return {
        "disposition": "padding",
        "source_mentions_fully_accounted": True,
        "records": [],
    }


def _valid_stage_a_output() -> dict:
    return {
        "slot_00": {"role": "record_anchor"},
        **{
            f"slot_{index:02d}": {"role": "padding"}
            for index in range(1, runner.STAGE_A_SLOT_COUNT)
        },
    }


def _valid_stage_b_output() -> dict:
    record_common = {
        "entity_type": "individual",
        "endorser_category": "private_individual",
        "title": None,
        "affiliation": "Teachers Association",
        "jurisdiction": None,
    }
    return {
        "slot_00": {
            "disposition": "records",
            "source_mentions_fully_accounted": True,
            "records": [
                {"name_parts": ["Alex", "Rivera"], **record_common},
                {"name_parts": ["Jamie", "Rivera"], **record_common},
            ],
        },
        "slot_01": _padding_decision(),
        "slot_02": _padding_decision(),
        "slot_03": _padding_decision(),
    }


def _args(stage: str) -> SimpleNamespace:
    return SimpleNamespace(
        stage=stage,
        seed=runner.GENERATION_SEED,
        max_model_len=runner.MAX_MODEL_LEN,
        max_output_tokens=runner.MAX_OUTPUT_TOKENS[stage],
        model_path=Path("/models") / runner.MODEL_REVISION,
        gpu_memory_utilization=runner.GPU_MEMORY_UTILIZATION,
        shard_index=None,
        shard_count=None,
    )


def _build_artifact(tmp_path: Path, batch: dict) -> tuple[Path, str]:
    root = tmp_path / "input"
    _write_artifact(
        output_dir=root,
        stage=batch["stage"],
        batches=[batch],
        selected_parent_ids=[batch["parent_document_id"]],
        provenance={"fixture": True},
        chunk_size=1,
    )
    return root, runner.file_sha256(root / "manifest.json")


def test_schema_dispatch_uses_target_count_and_fixed_stage_slots() -> None:
    stage_a = runner.schema_for_batch(_stage_a_batch())
    stage_b = runner.schema_for_batch(_stage_b_batch())

    assert list(stage_a["properties"]) == [
        f"slot_{index:02d}" for index in range(runner.STAGE_A_SLOT_COUNT)
    ]
    assert stage_a["properties"]["slot_00"]["properties"]["role"][
        "enum"
    ]
    assert stage_a["properties"]["slot_01"]["properties"]["role"] == {
        "type": "string",
        "const": "padding",
    }
    assert len(stage_b["properties"]) == runner.STAGE_B_SLOT_COUNT
    assert stage_b["properties"]["slot_01"]["properties"][
        "disposition"
    ]["const"] == "padding"

    mismatched = _stage_a_batch()
    mismatched["targets"].append(copy.deepcopy(mismatched["targets"][0]))
    with pytest.raises(ValueError, match="target slot and target counts"):
        runner.schema_for_batch(mismatched)


def test_prompt_rendering_is_deterministic_under_mapping_order() -> None:
    batch = _stage_a_batch()
    reordered = dict(reversed(list(batch.items())))

    first = runner.render_batch_prompt(batch)
    second = runner.render_batch_prompt(reordered)

    assert first == second
    assert first.startswith("ROUND4_DEVELOPMENT_BATCH_JSON\n{")
    assert runner.build_conversation(batch) == runner.build_conversation(batch)

    rendered = json.loads(first.split("\n", 1)[1])
    assert rendered == {
        "candidate": {"candidate_cycle_id": "candidate-a"},
        "padding_slots": [
            f"slot_{index:02d}"
            for index in range(1, runner.STAGE_A_SLOT_COUNT)
        ],
        "page": {"parent_document_id": "parent-a"},
        "target_lines": [
            {
                "line_index": 0,
                "line_text": "One Person",
                "slot": "slot_00",
                "source_line_id": "source-line-0",
            }
        ],
        "visible_lines": [{"line_index": 0, "line_text": "One Person"}],
    }
    assert "sha256" not in first
    assert "parent_start_char" not in first
    assert "deterministic_disposition" not in first


def test_compact_stage_a_view_preserves_ordered_source_and_target_identity() -> None:
    batch = _stage_a_batch()
    visible = [
        _line(4, "Repeated heading"),
        _line(5, "Alex Rivera"),
        _line(6, "Repeated heading"),
    ]
    batch["targets"] = [
        {"slot": "slot_00", **copy.deepcopy(visible[0])},
        {"slot": "slot_01", **copy.deepcopy(visible[2])},
    ]
    batch["target_slots"] = ["slot_00", "slot_01"]
    batch["padding_slots"] = [
        f"slot_{index:02d}" for index in range(2, runner.STAGE_A_SLOT_COUNT)
    ]
    batch["visible_lines"] = copy.deepcopy(visible)
    batch["full_parent_line_catalog_sha256"] = runner.sha256_json(visible)

    rendered = runner.compact_line_decision_model_view(batch)

    assert [
        (line["line_index"], line["line_text"])
        for line in rendered["visible_lines"]
    ] == [(4, "Repeated heading"), (5, "Alex Rivera"), (6, "Repeated heading")]
    assert [
        (
            line["slot"],
            line["source_line_id"],
            line["line_index"],
            line["line_text"],
        )
        for line in rendered["target_lines"]
    ] == [
        ("slot_00", "source-line-4", 4, "Repeated heading"),
        ("slot_01", "source-line-6", 6, "Repeated heading"),
    ]


def test_record_extraction_rendering_remains_the_full_authenticated_batch() -> None:
    batch = _stage_b_batch()

    assert runner.render_batch_prompt(batch) == (
        "ROUND4_DEVELOPMENT_BATCH_JSON\n" + runner.canonical_json(batch)
    )


@pytest.mark.skipif(
    not (REAL_FRESH_STAGE_A / "manifest.json").is_file(),
    reason="real fresh Stage-A development artifact is not present",
)
def test_real_fresh_stage_a_compact_view_is_below_prior_safe_maximum() -> None:
    manifest_path = REAL_FRESH_STAGE_A / "manifest.json"
    manifest, _, chunk_paths = runner.validate_input_manifest(
        manifest_path,
        REAL_FRESH_STAGE_A / "chunks",
        stage=runner.LINE_DECISION_STAGE,
        expected_manifest_sha256=runner.file_sha256(manifest_path),
    )
    batches = [
        batch
        for path in chunk_paths
        for batch in runner.read_gzip_jsonl(path)
    ]

    compact_sizes = [
        len(runner.render_batch_prompt(batch).encode("utf-8"))
        for batch in batches
    ]
    full_sizes = [
        len(
            (
                "ROUND4_DEVELOPMENT_BATCH_JSON\n"
                + runner.canonical_json(batch)
            ).encode("utf-8")
        )
        for batch in batches
    ]

    assert len(batches) == manifest["batch_count"]
    assert max(compact_sizes) < PRIOR_SAFE_USER_MESSAGE_MAX_BYTES
    assert max(compact_sizes) < max(full_sizes) / 4


def test_manifest_authenticates_builder_contract_and_every_chunk(
    tmp_path: Path,
) -> None:
    batch = _stage_a_batch()
    root, expected_sha = _build_artifact(tmp_path, batch)

    manifest, observed_sha, paths = runner.validate_input_manifest(
        root / "manifest.json",
        root / "chunks",
        stage=runner.LINE_DECISION_STAGE,
        expected_manifest_sha256=expected_sha,
    )

    assert observed_sha == expected_sha
    assert manifest["expected_batch_ids"] == [batch["batch_id"]]
    assert [path.name for path in paths] == ["chunk_00000.jsonl.gz"]


def test_manifest_expected_hash_is_checked_before_json_parsing(
    tmp_path: Path,
) -> None:
    root = tmp_path / "input"
    (root / "chunks").mkdir(parents=True)
    (root / "manifest.json").write_text("not json", encoding="utf-8")

    with pytest.raises(ValueError, match="expected SHA-256"):
        runner.validate_input_manifest(
            root / "manifest.json",
            root / "chunks",
            stage=runner.LINE_DECISION_STAGE,
            expected_manifest_sha256="0" * 64,
        )


def test_manifest_rejects_chunk_tampering_and_authorization_escalation(
    tmp_path: Path,
) -> None:
    batch = _stage_a_batch()
    root, expected_sha = _build_artifact(tmp_path, batch)
    chunk_path = root / "chunks" / "chunk_00000.jsonl.gz"
    chunk_path.write_bytes(chunk_path.read_bytes() + b"tamper")

    with pytest.raises(ValueError, match="byte count"):
        runner.validate_input_manifest(
            root / "manifest.json",
            root / "chunks",
            stage=runner.LINE_DECISION_STAGE,
            expected_manifest_sha256=expected_sha,
        )

    second_root, _ = _build_artifact(tmp_path / "second", batch)
    manifest_path = second_root / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["authorizations"]["human_truth"] = True
    unsigned = dict(manifest)
    unsigned.pop("manifest_payload_sha256")
    manifest["manifest_payload_sha256"] = runner.sha256_json(unsigned)
    manifest_path.write_text(
        runner.canonical_json(manifest) + "\n", encoding="utf-8"
    )

    with pytest.raises(ValueError, match="development-only authorizations"):
        runner.validate_input_manifest(
            manifest_path,
            second_root / "chunks",
            stage=runner.LINE_DECISION_STAGE,
            expected_manifest_sha256=runner.file_sha256(manifest_path),
        )


def test_stage_a_validation_is_strict_about_padding_and_coverage() -> None:
    batch = _stage_a_batch()
    valid = runner.validate_stage_output(batch, _valid_stage_a_output())
    assert valid == {
        "schema_valid": True,
        "coverage_complete": True,
        "source_projection_valid": True,
        "source_projection": {},
        "errors": [],
    }

    missing_padding = _valid_stage_a_output()
    missing_padding.pop("slot_15")
    invalid = runner.validate_stage_output(batch, missing_padding)
    assert invalid["schema_valid"] is False
    assert invalid["coverage_complete"] is False
    assert invalid["source_projection_valid"] is False

    target_as_padding = _valid_stage_a_output()
    target_as_padding["slot_00"] = {"role": "padding"}
    assert runner.validate_stage_output(batch, target_as_padding)[
        "schema_valid"
    ] is False


def test_stage_b_validation_projects_names_and_checks_category_entity_pair() -> None:
    batch = _stage_b_batch()
    output = _valid_stage_b_output()

    valid = runner.validate_stage_output(batch, output)

    assert valid["schema_valid"] is True
    assert valid["coverage_complete"] is True
    assert valid["source_projection_valid"] is True
    assert [
        record["canonical_name"]
        for record in valid["source_projection"]["slot_00"]
    ] == ["Alex Rivera", "Jamie Rivera"]
    assert all(
        len(record["name_part_spans"]) == 2
        for record in valid["source_projection"]["slot_00"]
    )

    bad_pair = copy.deepcopy(output)
    bad_pair["slot_00"]["records"][0]["entity_type"] = "organization"
    invalid = runner.validate_stage_output(batch, bad_pair)
    assert invalid["schema_valid"] is False
    assert invalid["source_projection_valid"] is False


def test_stage_b_validation_records_source_and_optional_field_failures() -> None:
    batch = _stage_b_batch()
    output = _valid_stage_b_output()
    output["slot_00"]["records"][0]["name_parts"] = ["Absent Name"]
    output["slot_00"]["records"][1]["title"] = "Mayor"

    validation = runner.validate_stage_output(batch, output)

    assert validation["schema_valid"] is True
    assert validation["coverage_complete"] is True
    assert validation["source_projection_valid"] is False
    assert any("no ordered source projection" in error for error in validation["errors"])
    assert any("not an exact contiguous" in error for error in validation["errors"])
    second = validation["source_projection"]["slot_00"][1]
    assert second["optional_field_changes"] == [
        {
            "field": "title",
            "original_value": "Mayor",
            "replacement_value": None,
            "reason": "not_exact_contiguous_visible_substring",
        }
    ]


def test_stage_b_validation_rejects_duplicate_overlapping_identity() -> None:
    batch = _stage_b_batch()
    output = _valid_stage_b_output()
    output["slot_00"]["records"][1] = copy.deepcopy(
        output["slot_00"]["records"][0]
    )

    validation = runner.validate_stage_output(batch, output)

    assert validation["schema_valid"] is True
    assert validation["coverage_complete"] is True
    assert validation["source_projection_valid"] is False
    assert any(
        "duplicate overlapping normalized identity" in error
        for error in validation["errors"]
    )


def test_structured_parameters_passes_exact_json_schema(monkeypatch) -> None:
    schema = runner.schema_for_batch(_stage_a_batch())
    sampling_module = types.ModuleType("vllm.sampling_params")

    class FakeStructuredOutputsParams:
        def __init__(self, *, json):
            self.json = json

    sampling_module.StructuredOutputsParams = FakeStructuredOutputsParams
    vllm_module = types.ModuleType("vllm")
    monkeypatch.setitem(sys.modules, "vllm", vllm_module)
    monkeypatch.setitem(sys.modules, "vllm.sampling_params", sampling_module)

    parameters = runner.structured_parameters(schema)

    assert set(parameters) == {"structured_outputs"}
    assert parameters["structured_outputs"].json == schema


def test_stage_a_output_binds_compact_message_and_full_authenticated_batch() -> None:
    batch = _stage_a_batch()
    args = _args(runner.LINE_DECISION_STAGE)
    manifest_sha = "d" * 64
    generation_sha = runner.generation_config_sha256(
        args, input_manifest_sha256=manifest_sha
    )
    raw_output = runner.canonical_json(_valid_stage_a_output())

    row = runner.derive_output_row(
        batch=batch,
        raw_output=raw_output,
        finish_reason="stop",
        stop_reason=None,
        output_tokens=16,
        prompt_tokens=32,
        args=args,
        input_manifest_sha256=manifest_sha,
        input_chunk_sha256="e" * 64,
        generation_config_sha256_value=generation_sha,
    )

    assert row["technical_pass"] is True
    assert row["input_batch_sha256"] == runner.sha256_json(batch)
    assert row["user_message_sha256"] == runner.sha256_text(
        runner.render_batch_prompt(batch)
    )
    assert row["input_batch_sha256"] != row["user_message_sha256"]


def test_fake_inference_persists_raw_output_and_never_overwrites(
    tmp_path: Path, monkeypatch
) -> None:
    batch = _stage_b_batch()
    root, manifest_sha = _build_artifact(tmp_path, batch)
    args = _args(runner.RECORD_EXTRACTION_STAGE)
    generation_sha = runner.generation_config_sha256(
        args, input_manifest_sha256=manifest_sha
    )
    raw_output = runner.canonical_json(_valid_stage_b_output())

    class FakeTokenizer:
        def apply_chat_template(self, *args, **kwargs):
            return [1, 2, 3, 4]

    class FakeCompletion:
        text = raw_output
        finish_reason = "stop"
        stop_reason = None
        token_ids = [11, 12]

    class FakeRequestOutput:
        outputs = [FakeCompletion()]

    class FakeLLM:
        def __init__(self):
            self.calls = []

        def chat(self, conversations, sampling_params, **kwargs):
            self.calls.append((conversations, sampling_params, kwargs))
            return [FakeRequestOutput()]

    sampling_calls = []

    def fake_sampling_params(**kwargs):
        sampling_calls.append(kwargs)
        return kwargs

    monkeypatch.setattr(
        runner,
        "structured_parameters",
        lambda schema: {"structured_outputs": {"json": schema}},
    )
    output_dir = tmp_path / "output"
    output_dir.mkdir()
    output_path = output_dir / "chunk_00000.out.jsonl.gz"
    input_path = root / "chunks" / "chunk_00000.jsonl.gz"
    input_chunk_sha = runner.file_sha256(input_path)
    llm = FakeLLM()

    with pytest.raises(ValueError, match="authenticated manifest"):
        runner.process_chunk(
            input_path=input_path,
            output_path=output_path,
            llm=llm,
            tokenizer=FakeTokenizer(),
            sampling_params_factory=fake_sampling_params,
            args=args,
            input_manifest_sha256=manifest_sha,
            expected_input_chunk_sha256="0" * 64,
            generation_config_sha256_value=generation_sha,
        )
    assert llm.calls == []

    rows = runner.process_chunk(
        input_path=input_path,
        output_path=output_path,
        llm=llm,
        tokenizer=FakeTokenizer(),
        sampling_params_factory=fake_sampling_params,
        args=args,
        input_manifest_sha256=manifest_sha,
        expected_input_chunk_sha256=input_chunk_sha,
        generation_config_sha256_value=generation_sha,
    )

    assert len(llm.calls) == 1
    assert len(sampling_calls) == 1
    assert sampling_calls[0]["structured_outputs"]["json"] == runner.schema_for_batch(
        batch
    )
    assert rows[0]["raw_output"] == raw_output
    assert rows[0]["input_chunk_sha256"] == input_chunk_sha
    assert rows[0]["input_batch_sha256"] == runner.sha256_json(batch)
    assert rows[0]["user_message_sha256"] == runner.sha256_text(
        runner.render_batch_prompt(batch)
    )
    assert rows[0]["model_output"] == _valid_stage_b_output()
    assert rows[0]["stage_validation"]["source_projection_valid"] is True
    assert rows[0]["technical_pass"] is True
    persisted = json.loads(gzip.decompress(output_path.read_bytes()).decode())
    assert persisted == rows[0]
    original_bytes = output_path.read_bytes()

    with pytest.raises(FileExistsError, match="overwrite"):
        runner.process_chunk(
            input_path=input_path,
            output_path=output_path,
            llm=llm,
            tokenizer=FakeTokenizer(),
            sampling_params_factory=fake_sampling_params,
            args=args,
            input_manifest_sha256=manifest_sha,
            expected_input_chunk_sha256=input_chunk_sha,
            generation_config_sha256_value=generation_sha,
        )
    assert output_path.read_bytes() == original_bytes


def test_generation_provenance_cannot_authorize_truth_expansion_or_release() -> None:
    args = _args(runner.LINE_DECISION_STAGE)
    manifest_sha = "d" * 64
    payload = runner.generation_config_payload(
        args, input_manifest_sha256=manifest_sha
    )

    assert payload["development_only"] is True
    assert payload["authorizations"] == runner.DEVELOPMENT_AUTHORIZATIONS
    assert payload["authorizations"]["human_truth"] is False
    assert payload["authorizations"]["full_expansion"] is False
    assert payload["authorizations"]["release"] is False
    assert payload["round4_development_binding"] == runner.ROUND4_DEVELOPMENT_BINDING
    assert payload["runner_version"] == runner.RUNNER_VERSION
    assert payload["user_message_rendering"] == (
        runner.LINE_DECISION_USER_MESSAGE_RENDERING
    )
    changed = copy.deepcopy(args)
    changed.gpu_memory_utilization = 0.91
    assert runner.generation_config_sha256(
        changed, input_manifest_sha256=manifest_sha
    ) != runner.generation_config_sha256(
        args, input_manifest_sha256=manifest_sha
    )


def test_sbatch_pins_blackwell_runtime_and_invokes_only_round4_runner() -> None:
    path = (
        runner.REPO
        / "scripts/hive/run_candidate_endorsement_round4_development.sbatch"
    )
    text = path.read_text(encoding="utf-8")

    subprocess.run(["bash", "-n", str(path)], check=True)
    assert "#SBATCH --gpus=6000_blackwell:1" in text
    assert runner.MODEL_REVISION in text
    assert runner.RUNTIME_LOCK_SHA256 in text
    assert "polsci-bakeoff-vllm-0.26.0-py312-cuda130-matched-v2.freeze.txt" in text
    assert "extract_candidate_endorsement_round4_development_vllm.py" in text
    assert "--expected-input-manifest-sha256" in text
    assert "--expected-generation-config-sha256" in text
    assert "extract_candidate_endorsement_line_ledger_vllm.py" not in text
    assert "source_corpus" not in text.casefold()
    assert "EXPECTED_SOURCE_CORPUS_SHA256" not in text
    assert "\nsbatch " not in text.casefold()
    assert "#SBATCH --requeue" not in text


def _context_stage_b_batch() -> dict:
    batch = _stage_b_batch()
    context = _line(0, "Context Only Person")
    prefix = context["line_text"] + "\n"
    owned = batch["targets"][0]["record_block_lines"][0]
    owned["source_line_id"] = "source-line-1"
    owned["parent_line_index"] = 1
    for field in ("parent_start_char", "parent_end_char", "parent_full_end_char"):
        owned[field] += len(prefix)
    from src.candidate_endorsement_line_ledger import LINE_INVENTORY_SPEC_SHA256
    for line in (context, owned):
        line["source_line_id"] = runner.sha256_json({
            "line_inventory_spec_sha256": LINE_INVENTORY_SPEC_SHA256,
            "parent_document_id": batch["parent_document_id"],
            **{key: line[key] for key in (
                "parent_line_index", "parent_start_char", "parent_end_char",
                "parent_full_end_char", "line_text_sha256", "line_terminator",
            )},
        })
    batch["targets"][0].update({key: value for key, value in owned.items()
                               if key != "slot"})
    source = prefix + owned["line_text"] + "\n"
    batch.update({
        "parent_source_text": source,
        "extraction_source_text_sha256": runner.sha256_text(source),
        "extraction_source_n_chars": len(source),
        "read_only_context_radius": 10,
        "read_only_context_policy": runner.READ_ONLY_CONTEXT_POLICY,
        "read_only_context_lines": [context],
        "read_only_context_lines_sha256": runner.sha256_json([context]),
    })
    return batch


def test_read_only_context_cannot_supply_names_and_default_rendering_unchanged() -> None:
    default = _stage_b_batch()
    assert runner.render_batch_prompt(default) == (
        "ROUND4_DEVELOPMENT_BATCH_JSON\n" + runner.canonical_json(default)
    )
    batch = _context_stage_b_batch()
    runner.render_batch_prompt(batch)
    output = _valid_stage_b_output()
    assert runner.validate_stage_output(batch, output)["source_projection_valid"]
    output["slot_00"]["records"][0]["name_parts"] = ["Context Only Person"]
    assert not runner.validate_stage_output(batch, output)["source_projection_valid"]


@pytest.mark.parametrize("tamper", ["source", "hash", "projection", "identity", "duplicate", "owned", "policy"])
def test_read_only_context_authentication(tamper: str) -> None:
    batch = _context_stage_b_batch()
    if tamper == "source":
        batch["parent_source_text"] += "tamper"
    elif tamper == "hash":
        batch["read_only_context_lines_sha256"] = "0" * 64
    elif tamper == "projection":
        batch["read_only_context_lines"][0]["parent_start_char"] = 1
    elif tamper == "identity":
        batch["read_only_context_lines"][0]["source_line_id"] = "0" * 64
    elif tamper == "duplicate":
        batch["read_only_context_lines"] *= 2
    elif tamper == "owned":
        batch["read_only_context_lines"].append(
            copy.deepcopy(batch["targets"][0]["record_block_lines"][0]))
    else:
        batch["read_only_context_policy"] = "allow_context_names"
    if tamper not in {"hash", "source", "policy"}:
        batch["read_only_context_lines_sha256"] = runner.sha256_json(
            batch["read_only_context_lines"])
    with pytest.raises(ValueError):
        runner.render_batch_prompt(batch)
