from __future__ import annotations

import copy
import gzip
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

import scripts.hive.extract_candidate_endorsement_line_ledger_vllm as runner

from scripts.hive.extract_candidate_endorsement_line_ledger_vllm import (
    DEFAULT_MAX_OUTPUT_TOKENS,
    PROMPT_TOKEN_COUNT_SPEC_SHA256,
    STRUCTURED_DECODING_BACKEND,
    _run_metadata,
    _prompt_token_count,
    _derive_output_row,
    generation_config_payload,
    generation_config_sha256,
    line_ledger_chunk_is_complete,
    llm_engine_kwargs,
)
from src.candidate_endorsement_line_ledger import (
    FROZEN_STAGE_A_V2_CONTRACT_SHA256,
    LINE_BATCH_SPEC_SHA256,
    LINE_ADJUDICATION_BATCH_SCHEMA_RULE_SHA256,
    LINE_ADJUDICATION_SLOT_COUNT,
    LINE_ADJUDICATION_SPEC_SHA256,
    LINE_ADJUDICATION_STAGE,
    LINE_DECISION_SLOT_COUNT,
    LINE_DECISION_STAGE,
    RECORD_EXTRACTION_STAGE,
    LINE_INVENTORY_SPEC_SHA256,
    get_line_ledger_contract,
    computed_recall40_stage_a2_contract,
    computed_recall40_stage_b_contract,
    schema_binding_fields_for_batch,
    sha256_json,
    sha256_text,
)
from src.candidate_extraction import MODEL_REVISION


def test_sbatch_places_vllm_and_torch_compile_caches_on_job_local_scratch() -> None:
    sbatch = (
        Path(__file__).resolve().parents[1]
        / "scripts/hive/run_candidate_endorsement_line_ledger.sbatch"
    ).read_text(encoding="utf-8")
    assert 'export VLLM_CACHE_ROOT="$SCRATCH_ROOT/vllm-cache"' in sbatch
    assert (
        'export TORCHINDUCTOR_CACHE_DIR="$SCRATCH_ROOT/torchinductor-cache"'
        in sbatch
    )
    assert '"$VLLM_CACHE_ROOT" "$TORCHINDUCTOR_CACHE_DIR"' in sbatch
    assert 'case "$job_local_cache" in' in sbatch


def _batch() -> dict[str, object]:
    line_id = sha256_text("Alice Smith")
    batch_id = sha256_text("fixed-stage-a-batch")
    target = {
        "slot": "slot_00",
        "source_line_id": line_id,
        "parent_line_index": 1,
        "parent_start_char": 13,
        "parent_end_char": 24,
        "parent_full_end_char": 25,
        "line_text": "Alice Smith",
        "line_text_sha256": sha256_text("Alice Smith"),
        "line_terminator": "\n",
        "deterministic_disposition": "model_decision_required",
        "model_decision_required": True,
        "inventory_unit_type": "complete_physical_line",
        "owner_segment_id": "segment",
        "owner_segment_index": 0,
        "complete_carrier_segment_ids": ["segment"],
        "segment_fragments": [],
    }
    batch = {
        "document_id": batch_id,
        "batch_id": batch_id,
        "stage": LINE_DECISION_STAGE,
        "route": "endorsements",
        "candidate_cycle_id": "H4TX00000-2024",
        "candidate_icpsr": "999999",
        "state": "TX",
        "office": "house",
        "year": 2024,
        "party": "democrat",
        "raw_text_sha256": "raw",
        "parent_document_id": "parent",
        "extraction_source_text_sha256": sha256_text(
            "Endorsements\nAlice Smith\n"
        ),
        "extraction_source_n_chars": 25,
        "source_urls": ["https://example.com/endorsements"],
        "parent_line_inventory_sha256": sha256_text("inventory"),
        "endorsement_line_context": {
            "source_scope": "parent_document",
            "context_text": "Endorsements",
            "qualifying_wayback_url": None,
        },
        "line_inventory_spec_sha256": LINE_INVENTORY_SPEC_SHA256,
        "line_batch_spec_sha256": LINE_BATCH_SPEC_SHA256,
        "batch_index_within_parent": 0,
        "target_slots": ["slot_00"],
        "padding_slots": [
            f"slot_{index:02d}" for index in range(1, LINE_DECISION_SLOT_COUNT)
        ],
        "targets": [target],
        "visible_lines": [target],
    }
    batch.update(schema_binding_fields_for_batch(batch))
    return batch


def test_stage_b_alignment_failure_is_technical_failure_and_not_restartable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    batch = {
        "document_id": sha256_text("stage-b-document"),
        "batch_id": sha256_text("stage-b-batch"),
        "stage": RECORD_EXTRACTION_STAGE,
        "route": "endorsements",
        "parent_document_id": sha256_text("stage-b-parent"),
        "candidate_cycle_id": "H4TX00000-2024",
    }
    binding = {
        "json_schema_catalog_sha256": sha256_text("catalog"),
        "json_schema_variant_id": "record_extraction_test",
        "json_schema_sha256": sha256_text("schema"),
        "target_slot_count": 1,
    }
    validation = {
        "schema_valid": True,
        "coverage_complete": True,
    }
    alignment = {
        "aligned_records": [],
        "withheld_records": [{"reason": "no_carrier"}],
        "aligned_record_count": 0,
        "withheld_record_count": 1,
        "public_coverage_complete": False,
    }
    monkeypatch.setattr(runner, "schema_binding_for_batch", lambda _: binding)
    monkeypatch.setattr(runner, "build_stage_user_message", lambda _: "prompt")
    monkeypatch.setattr(
        runner,
        "_stage_derivation",
        lambda _batch, _output, contract=None: (validation, alignment),
    )
    args = SimpleNamespace(max_model_len=32768, max_output_tokens=16384)
    contract = get_line_ledger_contract()
    manifest_hash = sha256_text("stage-b-manifest")
    generation_hash = sha256_text("stage-b-generation")
    row = _derive_output_row(
        batch=batch,
        raw_output="{}",
        finish_reason="stop",
        stop_reason=None,
        output_tokens=1,
        prompt_tokens=1,
        args=args,
        contract=contract,
        input_manifest_sha256=manifest_hash,
        generation_hash=generation_hash,
    )
    assert row["parse_status"] == "invalid_stage_alignment"
    assert row["technical_pass"] is False

    input_path = tmp_path / "chunk_00000.jsonl.gz"
    output_path = tmp_path / "chunk_00000.out.jsonl.gz"
    _write_gzip(input_path, [batch])
    _write_gzip(output_path, [row])
    assert not line_ledger_chunk_is_complete(
        input_path,
        output_path,
        stage=RECORD_EXTRACTION_STAGE,
        input_manifest_sha256=manifest_hash,
        expected_generation_config_sha256=generation_hash,
        expected_max_model_len=32768,
        expected_max_output_tokens=16384,
    )


def _model_output() -> dict[str, object]:
    return {
        f"slot_{index:02d}": {
            "role": "record_anchor" if index == 0 else "padding"
        }
        for index in range(LINE_DECISION_SLOT_COUNT)
    }


def _args() -> SimpleNamespace:
    return SimpleNamespace(
        stage=LINE_DECISION_STAGE,
        seed=20260825,
        max_model_len=32_768,
        max_output_tokens=DEFAULT_MAX_OUTPUT_TOKENS[LINE_DECISION_STAGE],
        model_path=Path("/models") / MODEL_REVISION,
        gpu_memory_utilization=0.9,
        shard_index=None,
        shard_count=None,
    )


def _write_gzip(path: Path, rows: list[dict[str, object]]) -> None:
    with gzip.open(path, "wt", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")


def _row(batch: dict[str, object]) -> tuple[dict[str, object], str, str]:
    args = _args()
    contract = get_line_ledger_contract()
    manifest_hash = sha256_text("manifest")
    generation_hash = generation_config_sha256(
        args, contract, input_manifest_sha256=manifest_hash
    )
    raw = json.dumps(_model_output(), ensure_ascii=False)
    row = _derive_output_row(
        batch=batch,
        raw_output=raw,
        finish_reason="stop",
        stop_reason=None,
        output_tokens=128,
        prompt_tokens=256,
        args=args,
        contract=contract,
        input_manifest_sha256=manifest_hash,
        generation_hash=generation_hash,
    )
    return row, manifest_hash, generation_hash


def test_generation_identity_binds_engine_level_fixed_whitespace() -> None:
    args = _args()
    kwargs = llm_engine_kwargs(args)
    assert kwargs["structured_outputs_config"] == {
        "backend": STRUCTURED_DECODING_BACKEND,
        "disable_any_whitespace": True,
    }
    contract = get_line_ledger_contract()
    first = generation_config_sha256(
        args, contract, input_manifest_sha256=sha256_text("manifest")
    )
    payload = generation_config_payload(
        args, contract, input_manifest_sha256=sha256_text("manifest")
    )
    assert payload["json_schema_catalog_sha256"] == contract[
        "json_schema_catalog_sha256"
    ][LINE_DECISION_STAGE]
    assert payload["json_schema_variant_sha256"] == contract[
        "json_schema_variant_sha256"
    ][LINE_DECISION_STAGE]
    assert payload["prompt_token_count_spec_sha256"] == (
        PROMPT_TOKEN_COUNT_SPEC_SHA256
    )
    changed = copy.copy(args)
    changed.max_output_tokens += 1
    assert first != generation_config_sha256(
        changed, contract, input_manifest_sha256=sha256_text("manifest")
    )


def test_prompt_token_count_uses_mapping_input_ids_not_mapping_length() -> None:
    class MappingTokenizer:
        def apply_chat_template(self, *_args, **_kwargs):
            return {"input_ids": [[101, 102, 103, 104]], "attention_mask": [[1] * 4]}

    assert _prompt_token_count(MappingTokenizer(), []) == 4
    assert PROMPT_TOKEN_COUNT_SPEC_SHA256

    class InvalidTokenizer:
        def apply_chat_template(self, *_args, **_kwargs):
            return {"attention_mask": [[1, 1]]}

    with pytest.raises(RuntimeError, match="context-budget attestation fails closed"):
        _prompt_token_count(InvalidTokenizer(), [])


def test_restart_replays_raw_parse_validation_and_exact_field_set(
    tmp_path: Path,
) -> None:
    batch = _batch()
    row, manifest_hash, generation_hash = _row(batch)
    input_path = tmp_path / "chunk_00000.jsonl.gz"
    output_path = tmp_path / "chunk_00000.out.jsonl.gz"
    _write_gzip(input_path, [batch])
    _write_gzip(output_path, [row])
    args = _args()
    kwargs = {
        "stage": LINE_DECISION_STAGE,
        "input_manifest_sha256": manifest_hash,
        "expected_generation_config_sha256": generation_hash,
        "expected_max_model_len": args.max_model_len,
        "expected_max_output_tokens": args.max_output_tokens,
    }
    assert line_ledger_chunk_is_complete(input_path, output_path, **kwargs)

    attacks: list[dict[str, object]] = []
    extra = copy.deepcopy(row)
    extra["unexpected"] = True
    attacks.append(extra)
    typed = copy.deepcopy(row)
    typed["output_tokens"] = 128.0
    attacks.append(typed)
    parsed = copy.deepcopy(row)
    parsed["model_output"]["slot_00"] = {"role": "nonrecord_content"}
    attacks.append(parsed)
    raw = copy.deepcopy(row)
    raw["raw_output"] = json.dumps({
        **_model_output(), "slot_00": {"role": "nonrecord_content"}
    })
    attacks.append(raw)
    schema_id = copy.deepcopy(row)
    schema_id["json_schema_variant_id"] = "line_decision_targets_16_v1"
    attacks.append(schema_id)
    schema_hash = copy.deepcopy(row)
    schema_hash["json_schema_sha256"] = sha256_text("wrong schema")
    attacks.append(schema_hash)
    catalog = copy.deepcopy(row)
    catalog["json_schema_catalog_sha256"] = sha256_text("wrong catalog")
    attacks.append(catalog)
    for attack in attacks:
        _write_gzip(output_path, [attack])
        assert not line_ledger_chunk_is_complete(input_path, output_path, **kwargs)


def test_restart_binds_ordered_prompt_token_counts(tmp_path: Path) -> None:
    batch = _batch()
    row, manifest_hash, generation_hash = _row(batch)
    input_path = tmp_path / "chunk_00000.jsonl.gz"
    output_path = tmp_path / "chunk_00000.out.jsonl.gz"
    _write_gzip(input_path, [batch])
    _write_gzip(output_path, [row])
    args = _args()
    replay = {
        "stage": LINE_DECISION_STAGE,
        "input_manifest_sha256": manifest_hash,
        "expected_generation_config_sha256": generation_hash,
        "expected_max_model_len": args.max_model_len,
        "expected_max_output_tokens": args.max_output_tokens,
        "expected_prompt_token_counts_sha256": sha256_json(
            [row["prompt_tokens_used"]]
        ),
    }
    assert line_ledger_chunk_is_complete(
        input_path, output_path, **replay
    )

    tampered = copy.deepcopy(row)
    tampered["prompt_tokens_used"] += 1
    tampered["context_margin_tokens"] -= 1
    _write_gzip(output_path, [tampered])
    assert not line_ledger_chunk_is_complete(
        input_path, output_path, **replay
    )

    _write_gzip(output_path, [row])
    round3_contract = copy.deepcopy(get_line_ledger_contract())
    round3_contract["recall40_round3_stage_a_scope_spec_sha256"] = (
        sha256_text("round-three scope")
    )
    assert not line_ledger_chunk_is_complete(
        input_path,
        output_path,
        contract_override=round3_contract,
        **replay,
    )
    replay.pop("expected_prompt_token_counts_sha256")
    assert not line_ledger_chunk_is_complete(
        input_path,
        output_path,
        contract_override=round3_contract,
        **replay,
    )


def test_restart_binds_entire_input_batch_and_user_message(tmp_path: Path) -> None:
    batch = _batch()
    row, manifest_hash, generation_hash = _row(batch)
    input_path = tmp_path / "chunk_00000.jsonl.gz"
    output_path = tmp_path / "chunk_00000.out.jsonl.gz"
    _write_gzip(output_path, [row])
    args = _args()
    kwargs = {
        "stage": LINE_DECISION_STAGE,
        "input_manifest_sha256": manifest_hash,
        "expected_generation_config_sha256": generation_hash,
        "expected_max_model_len": args.max_model_len,
        "expected_max_output_tokens": args.max_output_tokens,
    }
    reordered = copy.deepcopy(batch)
    reordered["source_urls"] = list(reversed([
        "https://example.com/endorsements", "https://example.com/supporters"
    ]))
    assert sha256_json(reordered) != row["input_batch_sha256"]
    _write_gzip(input_path, [reordered])
    assert not line_ledger_chunk_is_complete(input_path, output_path, **kwargs)


def _adjudication_batch(
    upstream_contract: str = FROZEN_STAGE_A_V2_CONTRACT_SHA256,
    batch_contract: dict[str, object] | None = None,
) -> dict[str, object]:
    if batch_contract is None:
        batch_contract = get_line_ledger_contract()
    base = _batch()
    original_target = copy.deepcopy(base["targets"][0])
    original_target.update({
        "parent_line_index": 0,
        "parent_start_char": 0,
        "parent_end_char": len(original_target["line_text"]),
        "parent_full_end_char": len(original_target["line_text"]),
        "line_terminator": "",
    })
    base["extraction_source_n_chars"] = len(original_target["line_text"])
    base["extraction_source_text_sha256"] = sha256_text(
        original_target["line_text"]
    )
    original_target["source_line_id"] = sha256_json({
        "line_inventory_spec_sha256": LINE_INVENTORY_SPEC_SHA256,
        "parent_document_id": base["parent_document_id"],
        "parent_line_index": original_target["parent_line_index"],
        "parent_start_char": original_target["parent_start_char"],
        "parent_end_char": original_target["parent_end_char"],
        "parent_full_end_char": original_target["parent_full_end_char"],
        "line_text_sha256": original_target["line_text_sha256"],
        "line_terminator": original_target["line_terminator"],
    })
    provenance_sha = sha256_json({"frozen_stage_a": "suffix-b"})
    eligibility = {
        "eligibility_version": "stage_a2_general_structural_eligibility_v1",
        "candidate_name_aliases": ["999999"],
        "candidate_self_eligible": False,
        "standalone_machine_json": False,
        "terminal_roles": [
            "record_anchor", "compound_record_anchor",
            "support_or_category_heading", "navigation_form_footer",
            "nonrecord_content",
        ],
        "continuation_component_anchor_source_line_ids": None,
    }
    exception_id = sha256_json({
        "line_adjudication_spec_sha256": LINE_ADJUDICATION_SPEC_SHA256,
        "upstream_stage_a_contract_sha256": (
            upstream_contract
        ),
        "parent_document_id": base["parent_document_id"],
        "source_line_id": original_target["source_line_id"],
        "stage_a_batch_id": "upstream-batch",
        "stage_a_slot": "slot_00",
        "stage_a_role": "unclear",
        "exception_reason": "stage_a_unclear",
        "allowed_anchor_source_line_ids": [],
        "deterministic_eligibility": eligibility,
    })
    target = {
        **original_target,
        "slot": "slot_00",
        "exception_id": exception_id,
        "exception_reason": "stage_a_unclear",
        "stage_a_decision": {
            "source_line_id": original_target["source_line_id"],
            "role": "unclear",
            "stage_a_batch_id": "upstream-batch",
            "stage_a_slot": "slot_00",
        },
        "allowed_anchors": [],
        "allowed_anchor_source_line_ids": [],
        "deterministic_eligibility": eligibility,
        "visible_source_line_ids": [original_target["source_line_id"]],
        "visible_source_line_ids_sha256": sha256_json([
            original_target["source_line_id"]
        ]),
        "stage_a_provenance_locator": {
            "input_batch_sha256": "a" * 64,
            "raw_output_sha256": "b" * 64,
            "model_output_sha256": "c" * 64,
        },
    }
    catalog = [{
        "source_line_id": original_target["source_line_id"],
        "parent_line_index": original_target["parent_line_index"],
        "parent_start_char": original_target["parent_start_char"],
        "parent_end_char": original_target["parent_end_char"],
        "parent_full_end_char": original_target["parent_full_end_char"],
        "line_text": original_target["line_text"],
        "line_text_sha256": original_target["line_text_sha256"],
        "line_terminator": original_target["line_terminator"],
        "model_decision_required": True,
        "stage_a_role": "unclear",
        "stage_a_batch_id": "upstream-batch",
        "stage_a_slot": "slot_00",
        "is_stage_a2_exception": True,
    }]
    batch = {
        **base,
        "stage": LINE_ADJUDICATION_STAGE,
        "line_adjudication_batch_schema_rule_sha256": (
            LINE_ADJUDICATION_BATCH_SCHEMA_RULE_SHA256
        ),
        "line_adjudication_spec_sha256": LINE_ADJUDICATION_SPEC_SHA256,
        "upstream_stage_a_contract_sha256": (
            upstream_contract
        ),
        "stage_a_provenance_sha256": provenance_sha,
        "parent_physical_line_count": 1,
        "target_slots": ["slot_00"],
        "padding_slots": [
            f"slot_{index:02d}"
            for index in range(1, LINE_ADJUDICATION_SLOT_COUNT)
        ],
        "source_line_catalog": catalog,
        "source_line_catalog_sha256": sha256_json(catalog),
        "targets": [target],
    }
    batch.pop("visible_lines", None)
    batch_id = sha256_json({
        "stage": LINE_ADJUDICATION_STAGE,
        "contract": batch_contract["line_ledger_contract_sha256"],
        "upstream_stage_a_contract": upstream_contract,
        "stage_a_provenance_sha256": provenance_sha,
        "parent_document_id": batch["parent_document_id"],
        "batch_index": batch["batch_index_within_parent"],
        "parent_physical_line_count": batch[
            "parent_physical_line_count"
        ],
        "exception_ids": [exception_id],
        "source_line_catalog_sha256": batch[
            "source_line_catalog_sha256"
        ],
    })
    batch["document_id"] = batch_id
    batch["batch_id"] = batch_id
    if upstream_contract != FROZEN_STAGE_A_V2_CONTRACT_SHA256:
        batch["recall40_stage_a2_contract_sha256"] = batch_contract[
            "line_ledger_contract_sha256"
        ]
    batch.update(schema_binding_fields_for_batch(batch))
    return batch


def _adjudication_model_output() -> dict[str, object]:
    return {
        f"slot_{index:02d}": (
            {
                "decision": "role",
                "final_role": "navigation_form_footer",
                "anchor_source_line_id": None,
            }
            if index == 0
            else {
                "decision": "padding",
                "final_role": None,
                "anchor_source_line_id": None,
            }
        )
        for index in range(LINE_ADJUDICATION_SLOT_COUNT)
    }


def _adjudication_args() -> SimpleNamespace:
    args = _args()
    args.stage = LINE_ADJUDICATION_STAGE
    args.max_output_tokens = DEFAULT_MAX_OUTPUT_TOKENS[LINE_ADJUDICATION_STAGE]
    return args


def test_adjudication_restart_and_metadata_bind_upstream_stage_a(
    tmp_path: Path,
) -> None:
    batch = _adjudication_batch()
    args = _adjudication_args()
    contract = get_line_ledger_contract()
    manifest_hash = sha256_text("stage-a2-manifest")
    generation_hash = generation_config_sha256(
        args, contract, input_manifest_sha256=manifest_hash
    )
    payload = generation_config_payload(
        args, contract, input_manifest_sha256=manifest_hash
    )
    assert payload["line_adjudication_spec_sha256"] == (
        LINE_ADJUDICATION_SPEC_SHA256
    )
    assert payload["upstream_stage_a_contract_sha256"] == (
        FROZEN_STAGE_A_V2_CONTRACT_SHA256
    )
    row = _derive_output_row(
        batch=batch,
        raw_output=json.dumps(_adjudication_model_output()),
        finish_reason="stop",
        stop_reason=None,
        output_tokens=128,
        prompt_tokens=256,
        args=args,
        contract=contract,
        input_manifest_sha256=manifest_hash,
        generation_hash=generation_hash,
    )
    assert row["technical_pass"] is True
    assert row["stage_a_provenance_sha256"] == batch[
        "stage_a_provenance_sha256"
    ]
    input_path = tmp_path / "chunk_00000.jsonl.gz"
    output_path = tmp_path / "chunk_00000.out.jsonl.gz"
    _write_gzip(input_path, [batch])
    _write_gzip(output_path, [row])
    replay = {
        "stage": LINE_ADJUDICATION_STAGE,
        "input_manifest_sha256": manifest_hash,
        "expected_generation_config_sha256": generation_hash,
        "expected_max_model_len": args.max_model_len,
        "expected_max_output_tokens": args.max_output_tokens,
    }
    assert line_ledger_chunk_is_complete(input_path, output_path, **replay)

    tampered = copy.deepcopy(row)
    tampered["stage_a_provenance_sha256"] = sha256_text("tampered")
    _write_gzip(output_path, [tampered])
    assert not line_ledger_chunk_is_complete(input_path, output_path, **replay)

    variant_id = batch["json_schema_variant_id"]
    variant_hash = batch["json_schema_sha256"]
    inventory = {
        "schema_variant_counts": {variant_id: 1},
        "schema_variant_hashes_used": {variant_id: variant_hash},
        "ordered_schema_variant_ids_sha256": sha256_json([variant_id]),
        "ordered_schema_variant_hashes_sha256": sha256_json([variant_hash]),
    }
    metadata = _run_metadata(
        args=args,
        contract=contract,
        generation_hash=generation_hash,
        input_manifest_sha256=manifest_hash,
        selected=1,
        selected_variant_inventory=inventory,
        selected_stage_a_provenance_sha256=[
            str(batch["stage_a_provenance_sha256"])
        ],
        rows=[],
        started="2026-08-25T00:00:00+00:00",
        recovery=True,
    )
    assert metadata["selected_stage_a_provenance_sha256"] == [
        batch["stage_a_provenance_sha256"]
    ]
    assert metadata["line_adjudication_spec_sha256"] == (
        LINE_ADJUDICATION_SPEC_SHA256
    )


def test_recall40_adjudication_row_restart_and_metadata_bind_dynamic_upstream(
    tmp_path: Path,
) -> None:
    upstream = get_line_ledger_contract()["line_ledger_contract_sha256"]
    contract = computed_recall40_stage_a2_contract(upstream)
    batch = _adjudication_batch(upstream, contract)
    args = _adjudication_args()
    args.upstream_stage_a_contract_sha256 = upstream
    manifest_hash = sha256_text("recall40-stage-a2-manifest")
    generation_hash = generation_config_sha256(
        args, contract, input_manifest_sha256=manifest_hash
    )
    row = _derive_output_row(
        batch=batch, raw_output=json.dumps(_adjudication_model_output()),
        finish_reason="stop", stop_reason=None, output_tokens=128,
        prompt_tokens=256, args=args, contract=contract,
        input_manifest_sha256=manifest_hash,
        generation_hash=generation_hash,
    )
    assert row["technical_pass"] is True
    assert row["upstream_stage_a_contract_sha256"] == upstream
    input_path = tmp_path / "chunk_00000.jsonl.gz"
    output_path = tmp_path / "chunk_00000.out.jsonl.gz"
    _write_gzip(input_path, [batch])
    _write_gzip(output_path, [row])
    assert line_ledger_chunk_is_complete(
        input_path, output_path, stage=LINE_ADJUDICATION_STAGE,
        input_manifest_sha256=manifest_hash,
        expected_generation_config_sha256=generation_hash,
        expected_max_model_len=args.max_model_len,
        expected_max_output_tokens=args.max_output_tokens,
        contract_override=contract,
    )
    variant_id = batch["json_schema_variant_id"]
    variant_hash = batch["json_schema_sha256"]
    metadata = _run_metadata(
        args=args, contract=contract, generation_hash=generation_hash,
        input_manifest_sha256=manifest_hash, selected=1,
        selected_variant_inventory={
            "schema_variant_counts": {variant_id: 1},
            "schema_variant_hashes_used": {variant_id: variant_hash},
            "ordered_schema_variant_ids_sha256": sha256_json([variant_id]),
            "ordered_schema_variant_hashes_sha256": sha256_json([variant_hash]),
        },
        selected_stage_a_provenance_sha256=[
            str(batch["stage_a_provenance_sha256"])
        ], rows=[], started="2026-08-27T00:00:00+00:00", recovery=True,
    )
    assert metadata["upstream_stage_a_contract_sha256"] == upstream


def test_recall40_stage_b_generation_and_metadata_are_two_pass_only() -> None:
    contract = computed_recall40_stage_b_contract()
    args = _args()
    args.stage = RECORD_EXTRACTION_STAGE
    args.max_output_tokens = DEFAULT_MAX_OUTPUT_TOKENS[RECORD_EXTRACTION_STAGE]
    manifest_hash = sha256_text("recall40-stage-b-manifest")
    payload = generation_config_payload(
        args, contract, input_manifest_sha256=manifest_hash
    )
    assert payload["stage_b_two_pass_provenance_spec_sha256"] == contract[
        "stage_b_two_pass_provenance_spec_sha256"
    ]
    assert "stage_b_three_pass_provenance_spec_sha256" not in payload
    generation_hash = generation_config_sha256(
        args, contract, input_manifest_sha256=manifest_hash
    )
    metadata = _run_metadata(
        args=args, contract=contract, generation_hash=generation_hash,
        input_manifest_sha256=manifest_hash, selected=0,
        selected_variant_inventory={
            "schema_variant_counts": {}, "schema_variant_hashes_used": {},
            "ordered_schema_variant_ids_sha256": sha256_json([]),
            "ordered_schema_variant_hashes_sha256": sha256_json([]),
        },
        selected_stage_a_provenance_sha256=[], rows=[],
        started="2026-08-27T00:00:00+00:00", recovery=True,
    )
    assert metadata["stage_b_two_pass_provenance_spec_sha256"] == contract[
        "stage_b_two_pass_provenance_spec_sha256"
    ]
    assert "stage_b_three_pass_provenance_spec_sha256" not in metadata
