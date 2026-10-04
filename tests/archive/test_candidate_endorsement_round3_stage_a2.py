from __future__ import annotations

import copy
import gzip
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from scripts.build_candidate_endorsement_line_ledger import (
    DEFAULT_FULL_EXPORT_DIR,
    build_recall40_round3_line_adjudication_export,
    load_recall40_round3_stage_a_run,
)
from scripts.hive.extract_candidate_endorsement_line_ledger_vllm import (
    DEFAULT_MAX_OUTPUT_TOKENS,
    FROZEN_GENERATION_SEED,
    _derive_output_row,
    _run_metadata,
    file_sha256,
    generation_config_sha256,
    line_ledger_chunk_is_complete,
    read_gzip_jsonl,
    schema_variant_inventory,
    validate_input_manifest,
)
from src.candidate_endorsement_line_ledger import (
    FROZEN_RECALL40_ROUND3_STAGE_A_CONTRACT_SHA256,
    FROZEN_RECALL40_ROUND3_STAGE_A_GENERATION_CONFIG_SHA256,
    FROZEN_RECALL40_ROUND3_STAGE_A_INPUT_MANIFEST_SHA256,
    FROZEN_RECALL40_ROUND3_STAGE_A2_CONTRACT_SHA256,
    FROZEN_STAGE_A2_V4_CONTRACT_SHA256,
    LINE_ADJUDICATION_SLOT_COUNT,
    LINE_ADJUDICATION_SPEC_SHA256,
    LINE_ADJUDICATION_STAGE,
    LINE_INVENTORY_SPEC_SHA256,
    RECALL40_ROUND3_LINE_ADJUDICATION_PROMPT,
    RECALL40_ROUND3_STAGE_A2_BATCH_SPEC_SHA256,
    RECALL40_ROUND3_STAGE_A2_SCHEMA_RULE_SHA256,
    RECALL40_ROUND3_STAGE_A2_SPEC_SHA256,
    build_recall40_round3_line_adjudication_batches,
    build_stage_user_message,
    canonical_round3_line_adjudication_relations,
    computed_recall40_round3_stage_a2_contract,
    get_frozen_recall40_round3_stage_a2_contract,
    get_frozen_stage_a2_v4_contract,
    line_adjudication_exception_identity_ledger,
    prompt_for_contract_stage,
    round3_line_adjudication_exception_ledger,
    sha256_json,
    sha256_text,
    validate_line_adjudication_output,
)


REPO = Path(__file__).resolve().parents[1]
ROUND3_STAGE_A_EXPORT = (
    REPO
    / "build/hive_candidate_extraction/"
    "v2_1_endorsement_recall40_round3_stage_a_preflight_20260906a"
)
ROUND3_STAGE_A_OUTPUT = (
    REPO
    / "build/hive_candidate_extraction/"
    "v2_1_endorsement_recall40_round3_stage_a_outputs_20260906a"
)


def _line(parent_id: str, index: int, text: str, cursor: int, *, final: bool):
    terminator = "" if final else "\n"
    end = cursor + len(text)
    full_end = end + len(terminator)
    text_hash = sha256_text(text)
    line_id = sha256_json({
        "line_inventory_spec_sha256": LINE_INVENTORY_SPEC_SHA256,
        "parent_document_id": parent_id,
        "parent_line_index": index,
        "parent_start_char": cursor,
        "parent_end_char": end,
        "parent_full_end_char": full_end,
        "line_text_sha256": text_hash,
        "line_terminator": terminator,
    })
    return {
        "source_line_id": line_id,
        "parent_line_index": index,
        "parent_start_char": cursor,
        "parent_end_char": end,
        "parent_full_end_char": full_end,
        "line_text": text,
        "line_text_sha256": text_hash,
        "line_terminator": terminator,
        "model_decision_required": True,
    }


def _fixture():
    parent_id = "round3-a2-test-parent"
    texts = [
        "Endorsements",
        "Umbrella Union:",
        "Local 100",
        "Alice Candidate",
        "Testimonial text",
        "Footer",
    ]
    lines = []
    cursor = 0
    for index, text in enumerate(texts):
        line = _line(
            parent_id, index, text, cursor, final=index == len(texts) - 1
        )
        lines.append(line)
        cursor = line["parent_full_end_char"]
    parent_text = "\n".join(texts)
    parent = {
        "candidate_cycle_id": "H4ZZ00000-2024",
        "candidate_icpsr": "Candidate, Alice",
        "state": "ZZ",
        "office": "house",
        "year": 2024,
        "party": "independent",
        "raw_text_sha256": sha256_text(parent_text),
        "parent_document_id": parent_id,
        "extraction_source_text_sha256": sha256_text(parent_text),
        "extraction_source_n_chars": len(parent_text),
        "source_urls": ["https://example.test/endorsements"],
        "parent_line_inventory_sha256": sha256_text("test-inventory"),
        "endorsement_line_context": {
            "source_scope": "parent_document",
            "context_text": "Endorsements",
            "qualifying_wayback_url": None,
        },
        "lines": lines,
    }
    roles = [
        "support_or_category_heading",
        "record_anchor",
        "unclear",
        "candidate_self",
        "record_continuation_previous",
        "nonrecord_content",
    ]
    stage_a_batch_id = sha256_text("round3-stage-a-batch")
    decisions = {
        line["source_line_id"]: {
            "source_line_id": line["source_line_id"],
            "role": role,
            "stage_a_batch_id": stage_a_batch_id,
            "stage_a_slot": f"slot_{index:02d}",
        }
        for index, (line, role) in enumerate(zip(lines, roles, strict=True))
    }
    locator = {
        "input_chunk_file": "chunks/chunk_00000.jsonl.gz",
        "input_chunk_sha256": sha256_text("input-chunk"),
        "output_chunk_file": "chunk_00000.out.jsonl.gz",
        "output_chunk_sha256": sha256_text("output-chunk"),
        "input_batch_sha256": sha256_text("input-batch"),
        "raw_output_sha256": sha256_text("raw-output"),
        "model_output_sha256": sha256_text("model-output"),
        "generation_config_sha256": (
            FROZEN_RECALL40_ROUND3_STAGE_A_GENERATION_CONFIG_SHA256
        ),
    }
    provenance_payload = {
        "stage_a_input_manifest_sha256": (
            FROZEN_RECALL40_ROUND3_STAGE_A_INPUT_MANIFEST_SHA256
        ),
        "stage_a_line_ledger_contract_sha256": (
            FROZEN_RECALL40_ROUND3_STAGE_A_CONTRACT_SHA256
        ),
        "stage_a_generation_config_sha256": (
            FROZEN_RECALL40_ROUND3_STAGE_A_GENERATION_CONFIG_SHA256
        ),
        "stage_a_ordered_output_batch_ids_sha256": sha256_text(
            "ordered-output-batches"
        ),
        "source_line_decision_ledger_sha256": sha256_json(decisions),
        "validated_output_batches": 1,
        "input_files": [],
        "output_files": [],
        "run_metadata_files": [],
        "typed_restart_replay_complete": True,
        "selected_parent_ids_sha256": sha256_json([parent_id]),
        "by_batch_id": {stage_a_batch_id: locator},
    }
    provenance = {
        **provenance_payload,
        "stage_a_provenance_sha256": sha256_json(provenance_payload),
    }
    return parent, decisions, provenance


def _valid_output(batch):
    output = {}
    for target in batch["targets"]:
        reason = target["exception_reason"]
        if reason == "stage_a_unclear":
            decision, role = "role", "record_anchor"
        elif reason == "stage_a_candidate_self_review":
            decision, role = "role", "candidate_self"
        else:
            decision, role = "not_continuation", "nonrecord_content"
        output[target["slot"]] = {
            "decision": decision,
            "final_role": role,
            "anchor_source_line_id": None,
        }
    for slot in batch["padding_slots"]:
        output[slot] = {
            "decision": "padding",
            "final_role": None,
            "anchor_source_line_id": None,
        }
    return output


def _args(batch):
    return SimpleNamespace(
        stage=LINE_ADJUDICATION_STAGE,
        seed=FROZEN_GENERATION_SEED,
        max_model_len=32768,
        max_output_tokens=DEFAULT_MAX_OUTPUT_TOKENS[
            LINE_ADJUDICATION_STAGE
        ],
        upstream_stage_a_input_manifest_sha256=batch[
            "upstream_stage_a_input_manifest_sha256"
        ],
        upstream_stage_a_generation_config_sha256=batch[
            "upstream_stage_a_generation_config_sha256"
        ],
        upstream_stage_a_output_batch_ids_sha256=batch[
            "upstream_stage_a_output_batch_ids_sha256"
        ],
        upstream_stage_a_decision_ledger_sha256=batch[
            "upstream_stage_a_decision_ledger_sha256"
        ],
        stage_a_provenance_sha256=batch["stage_a_provenance_sha256"],
        shard_index=0,
        shard_count=1,
    )


def _write_gzip(path: Path, rows):
    with gzip.open(path, "wt", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")


def _rewrite_artifact(
    destination: Path,
    manifest: dict,
    batches: list[dict],
) -> None:
    chunk_path = destination / "chunks/chunk_00000.jsonl.gz"
    _write_gzip(chunk_path, batches)
    batch_ids = [str(batch["batch_id"]) for batch in batches]
    variant_ids = [str(batch["json_schema_variant_id"]) for batch in batches]
    variant_hashes = [str(batch["json_schema_sha256"]) for batch in batches]
    manifest["files"]["chunks"] = [{
        "file": chunk_path.name,
        "records": len(batches),
        "bytes": chunk_path.stat().st_size,
        "sha256": file_sha256(chunk_path),
        "content_sha256": sha256_json(batches),
        "first_document_id": batch_ids[0],
        "last_document_id": batch_ids[-1],
    }]
    manifest["expected_batch_ids"] = batch_ids
    manifest["expected_batch_ids_sha256"] = sha256_json(batch_ids)
    manifest["ordered_schema_variant_ids_sha256"] = sha256_json(variant_ids)
    manifest["ordered_schema_variant_hashes_sha256"] = sha256_json(
        variant_hashes
    )
    identity = line_adjudication_exception_identity_ledger(batches)
    manifest["stage_a_exception_identity_ledger"] = identity
    manifest["stage_a_exception_identity_ledger_sha256"] = sha256_json(
        identity
    )
    manifest.pop("manifest_payload_sha256", None)
    manifest["manifest_payload_sha256"] = sha256_json(manifest)
    (destination / "manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def test_round3_a2_contract_is_stage_specific_and_frozen():
    contract = computed_recall40_round3_stage_a2_contract()
    assert contract["active_stage"] == LINE_ADJUDICATION_STAGE
    assert set(contract["prompt_sha256"]) == {LINE_ADJUDICATION_STAGE}
    assert set(contract["json_schema_catalog_sha256"]) == {
        LINE_ADJUDICATION_STAGE
    }
    assert contract["line_batch_spec_sha256"] == (
        RECALL40_ROUND3_STAGE_A2_BATCH_SPEC_SHA256
    )
    assert contract["line_adjudication_spec_sha256"] == (
        RECALL40_ROUND3_STAGE_A2_SPEC_SHA256
    )
    assert contract["line_adjudication_batch_schema_rule_sha256"] == (
        RECALL40_ROUND3_STAGE_A2_SCHEMA_RULE_SHA256
    )
    assert prompt_for_contract_stage(contract, LINE_ADJUDICATION_STAGE) == (
        RECALL40_ROUND3_LINE_ADJUDICATION_PROMPT
    )
    assert not any(
        "stage_b" in key or "stage_a3" in key or "record_extraction" in key
        or "human" in key or "name" in key
        for key in contract
    )
    assert FROZEN_RECALL40_ROUND3_STAGE_A2_CONTRACT_SHA256 == (
        "e33c720243f3c498feec103f07b39df5e7705be60ecc551442d27249bda85f17"
    )
    assert get_frozen_recall40_round3_stage_a2_contract() == contract


def test_round3_a2_review_override_is_scope_restricted(
    tmp_path: Path,
):
    chunks = tmp_path / "chunks"
    chunks.mkdir()
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(json.dumps({
        "mode": "line_adjudication_recall40_round3",
    }))
    contract = computed_recall40_round3_stage_a2_contract()
    wrong_contract = copy.deepcopy(contract)
    wrong_contract["line_ledger_contract_sha256"] = "0" * 64
    with pytest.raises(ValueError, match="production contract differs"):
        validate_input_manifest(
            manifest_path,
            chunks,
            stage=LINE_ADJUDICATION_STAGE,
            contract=wrong_contract,
        )
    manifest_path.write_text(json.dumps({
        "mode": "line_adjudication_recall40_round2",
    }))
    with pytest.raises(ValueError, match="unfrozen review is restricted"):
        validate_input_manifest(
            manifest_path,
            chunks,
            stage=LINE_ADJUDICATION_STAGE,
            contract=contract,
            allow_unfrozen_review=True,
        )


def test_round3_a2_complete_parent_schema_output_restart_and_metadata(
    tmp_path: Path,
):
    parent, decisions, provenance = _fixture()
    exceptions = round3_line_adjudication_exception_ledger(
        [parent], decisions
    )
    assert [row["exception_reason"] for row in exceptions] == [
        "stage_a_unclear",
        "stage_a_candidate_self_review",
        "stage_a_unowned_continuation",
    ]
    batches = build_recall40_round3_line_adjudication_batches(
        [parent], decisions, stage_a_provenance=provenance
    )
    assert len(batches) == 1
    batch = batches[0]
    full_ids = [line["source_line_id"] for line in parent["lines"]]
    assert [line["source_line_id"] for line in batch[
        "source_line_catalog"
    ]] == full_ids
    assert all(
        target["visible_source_line_ids"] == full_ids
        for target in batch["targets"]
    )
    assert batch["json_schema_variant_id"].startswith(
        "recall40_round3_line_adjudication_profile_"
    )
    output = _valid_output(batch)
    assert validate_line_adjudication_output(batch, output)["schema_valid"]
    message = build_stage_user_message(batch)
    assert "Source-line catalog" in message
    relations = canonical_round3_line_adjudication_relations(batch)
    allowed_ids = {
        str(item["anchor_source_line_id"])
        for items in relations["allowed_anchors_by_slot"].values()
        for item in items
    }
    assert allowed_ids
    assert all(line_id in message for line_id in allowed_ids)
    nondecision_id = str(parent["lines"][-1]["source_line_id"])
    assert nondecision_id not in allowed_ids
    assert nondecision_id not in message

    args = _args(batch)
    contract = computed_recall40_round3_stage_a2_contract()
    manifest_hash = sha256_text("round3-a2-manifest")
    generation_hash = generation_config_sha256(
        args, contract, input_manifest_sha256=manifest_hash
    )
    row = _derive_output_row(
        batch=batch,
        raw_output=json.dumps(output),
        finish_reason="stop",
        stop_reason=None,
        output_tokens=64,
        prompt_tokens=512,
        args=args,
        contract=contract,
        input_manifest_sha256=manifest_hash,
        generation_hash=generation_hash,
    )
    for field in (
        "upstream_stage_a_input_manifest_sha256",
        "upstream_stage_a_generation_config_sha256",
        "upstream_stage_a_output_batch_ids_sha256",
        "upstream_stage_a_decision_ledger_sha256",
        "stage_a_provenance_sha256",
    ):
        assert row[field] == batch[field]
    input_path = tmp_path / "chunk_00000.jsonl.gz"
    output_path = tmp_path / "chunk_00000.out.jsonl.gz"
    _write_gzip(input_path, [batch])
    _write_gzip(output_path, [row])
    assert not line_ledger_chunk_is_complete(
        input_path,
        output_path,
        stage=LINE_ADJUDICATION_STAGE,
        input_manifest_sha256=manifest_hash,
        expected_generation_config_sha256=generation_hash,
        expected_max_model_len=32768,
        expected_max_output_tokens=DEFAULT_MAX_OUTPUT_TOKENS[
            LINE_ADJUDICATION_STAGE
        ],
        contract_override=contract,
    )
    metadata = _run_metadata(
        args=args,
        contract=contract,
        generation_hash=generation_hash,
        input_manifest_sha256=manifest_hash,
        selected=1,
        selected_variant_inventory=schema_variant_inventory([batch]),
        selected_stage_a_provenance_sha256=[
            batch["stage_a_provenance_sha256"]
        ],
        rows=[row],
        started="2026-09-06T12:00:00+00:00",
        recovery=False,
    )
    assert metadata["stage_a_provenance_sha256"] == batch[
        "stage_a_provenance_sha256"
    ]
    assert metadata["generation_config"][
        "upstream_stage_a_output_batch_ids_sha256"
    ] == batch["upstream_stage_a_output_batch_ids_sha256"]


def test_round3_a2_rejects_reason_context_and_upstream_identity_tamper():
    parent, decisions, provenance = _fixture()
    batch = build_recall40_round3_line_adjudication_batches(
        [parent], decisions, stage_a_provenance=provenance
    )[0]
    reason_attack = copy.deepcopy(batch)
    reason_attack["targets"][0]["exception_reason"] = (
        "stage_a_candidate_self_review"
    )
    with pytest.raises(ValueError, match="canonical target"):
        build_stage_user_message(reason_attack)

    context_attack = copy.deepcopy(batch)
    context_attack["source_line_catalog"][-1]["line_text"] = "Injected"
    with pytest.raises(ValueError, match="physical line"):
        build_stage_user_message(context_attack)

    upstream_attack = copy.deepcopy(batch)
    upstream_attack["upstream_stage_a_output_batch_ids_sha256"] = "0" * 64
    with pytest.raises(ValueError, match="batch identity"):
        build_stage_user_message(upstream_attack)


def test_round3_a2_absent_real_outputs_fail_before_artifact_creation(
    tmp_path: Path,
):
    missing_outputs = tmp_path / "absent-stage-a-outputs"
    with pytest.raises(FileNotFoundError, match="run metadata is absent"):
        load_recall40_round3_stage_a_run(
            ROUND3_STAGE_A_EXPORT, missing_outputs
        )
    destination = tmp_path / "a2-shell"
    with pytest.raises(FileNotFoundError, match="run metadata is absent"):
        build_recall40_round3_line_adjudication_export(
            DEFAULT_FULL_EXPORT_DIR,
            ROUND3_STAGE_A_EXPORT,
            missing_outputs,
            destination,
        )
    assert not destination.exists()


def test_round3_a2_real_stage_a_run_builds_valid_frozen_artifact(
    tmp_path: Path,
):
    destination = tmp_path / "round3-a2-frozen"
    manifest = build_recall40_round3_line_adjudication_export(
        DEFAULT_FULL_EXPORT_DIR,
        ROUND3_STAGE_A_EXPORT,
        ROUND3_STAGE_A_OUTPUT,
        destination,
    )
    assert manifest["contract_is_frozen"] is True
    assert manifest["frozen_line_ledger_contract_sha256"] == (
        FROZEN_RECALL40_ROUND3_STAGE_A2_CONTRACT_SHA256
    )
    assert manifest["stage_a_exception_count"] == 11
    assert manifest["stage_a_exception_reason_counts"] == {
        "stage_a_unowned_continuation": 11,
    }
    assert manifest["expected_stage_a2_batch_count"] == 3
    assert manifest["exported_records"] == 3

    validated, _manifest_sha256, input_paths = validate_input_manifest(
        destination / "manifest.json",
        destination / "chunks",
        stage=LINE_ADJUDICATION_STAGE,
        contract=get_frozen_recall40_round3_stage_a2_contract(),
    )
    assert validated == manifest
    assert [path.name for path in input_paths] == ["chunk_00000.jsonl.gz"]


def test_round3_a2_frozen_artifact_rejects_rehashed_upstream_tamper(
    tmp_path: Path,
):
    destination = tmp_path / "round3-a2-frozen"
    manifest = build_recall40_round3_line_adjudication_export(
        DEFAULT_FULL_EXPORT_DIR,
        ROUND3_STAGE_A_EXPORT,
        ROUND3_STAGE_A_OUTPUT,
        destination,
    )
    attack = copy.deepcopy(manifest)
    attack["upstream_stage_a_decision_ledger_sha256"] = "0" * 64
    attack.pop("manifest_payload_sha256")
    attack["manifest_payload_sha256"] = sha256_json(attack)
    (destination / "manifest.json").write_text(
        json.dumps(attack, ensure_ascii=False, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    with pytest.raises(
        ValueError,
        match="upstream_stage_a_decision_ledger_sha256 differs",
    ):
        validate_input_manifest(
            destination / "manifest.json",
            destination / "chunks",
            stage=LINE_ADJUDICATION_STAGE,
            contract=get_frozen_recall40_round3_stage_a2_contract(),
        )

    gate_attack = copy.deepcopy(manifest)
    gate_attack["corrected_71c_issue_audit_gate"] = {}
    gate_attack.pop("manifest_payload_sha256")
    gate_attack["manifest_payload_sha256"] = sha256_json(gate_attack)
    (destination / "manifest.json").write_text(
        json.dumps(gate_attack, ensure_ascii=False, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="fixed scope differs"):
        validate_input_manifest(
            destination / "manifest.json",
            destination / "chunks",
            stage=LINE_ADJUDICATION_STAGE,
            contract=get_frozen_recall40_round3_stage_a2_contract(),
        )

    count_paths = (
        ("chunk_size",),
        ("expected_stage_a2_batch_count",),
        ("expected_stage_batch_count",),
        ("exported_records",),
        ("files", "selection", "bytes"),
        ("files", "selection", "rows"),
        ("files", "chunks", 0, "bytes"),
        ("files", "chunks", 0, "records"),
    )
    for path in count_paths:
        count_attack = copy.deepcopy(manifest)
        target = count_attack
        for key in path[:-1]:
            target = target[key]
        key = path[-1]
        target[key] = float(target[key])
        count_attack.pop("manifest_payload_sha256")
        count_attack["manifest_payload_sha256"] = sha256_json(count_attack)
        (destination / "manifest.json").write_text(
            json.dumps(count_attack, ensure_ascii=False, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        with pytest.raises(ValueError, match="count type differs"):
            validate_input_manifest(
                destination / "manifest.json",
                destination / "chunks",
                stage=LINE_ADJUDICATION_STAGE,
                contract=get_frozen_recall40_round3_stage_a2_contract(),
            )


def test_round3_a2_rejects_coordinated_batch_provenance_and_order_attacks(
    tmp_path: Path,
):
    destination = tmp_path / "round3-a2-frozen"
    baseline_manifest = build_recall40_round3_line_adjudication_export(
        DEFAULT_FULL_EXPORT_DIR,
        ROUND3_STAGE_A_EXPORT,
        ROUND3_STAGE_A_OUTPUT,
        destination,
    )
    input_path = destination / "chunks/chunk_00000.jsonl.gz"
    baseline_batches = read_gzip_jsonl(input_path)
    contract = computed_recall40_round3_stage_a2_contract()

    provenance_manifest = copy.deepcopy(baseline_manifest)
    provenance_batches = copy.deepcopy(baseline_batches)
    attacked = provenance_batches[0]
    attacked["upstream_stage_a_output_batch_ids_sha256"] = "0" * 64
    identity = {
        "stage": LINE_ADJUDICATION_STAGE,
        "contract": contract["line_ledger_contract_sha256"],
        "upstream_stage_a_contract_sha256": attacked[
            "upstream_stage_a_contract_sha256"
        ],
        "upstream_stage_a_input_manifest_sha256": attacked[
            "upstream_stage_a_input_manifest_sha256"
        ],
        "upstream_stage_a_generation_config_sha256": attacked[
            "upstream_stage_a_generation_config_sha256"
        ],
        "upstream_stage_a_output_batch_ids_sha256": attacked[
            "upstream_stage_a_output_batch_ids_sha256"
        ],
        "upstream_stage_a_decision_ledger_sha256": attacked[
            "upstream_stage_a_decision_ledger_sha256"
        ],
        "stage_a_provenance_sha256": attacked[
            "stage_a_provenance_sha256"
        ],
        "parent_document_id": attacked["parent_document_id"],
        "batch_index": attacked["batch_index_within_parent"],
        "exception_ids": [
            target["exception_id"] for target in attacked["targets"]
        ],
        "source_line_catalog_sha256": attacked[
            "source_line_catalog_sha256"
        ],
    }
    attacked_id = sha256_json(identity)
    attacked["batch_id"] = attacked_id
    attacked["document_id"] = attacked_id
    _rewrite_artifact(
        destination, provenance_manifest, provenance_batches
    )
    with pytest.raises(ValueError, match="batch provenance differs"):
        validate_input_manifest(
            destination / "manifest.json",
            destination / "chunks",
            stage=LINE_ADJUDICATION_STAGE,
            contract=contract,
        )

    order_manifest = copy.deepcopy(baseline_manifest)
    order_batches = list(reversed(copy.deepcopy(baseline_batches)))
    _rewrite_artifact(destination, order_manifest, order_batches)
    with pytest.raises(ValueError, match="canonical batch order differs"):
        validate_input_manifest(
            destination / "manifest.json",
            destination / "chunks",
            stage=LINE_ADJUDICATION_STAGE,
            contract=contract,
        )

    metadata_manifest = copy.deepcopy(baseline_manifest)
    metadata_batches = copy.deepcopy(baseline_batches)
    context = metadata_batches[0]["endorsement_line_context"]
    context_text = str(context["context_text"])
    assert context_text
    replacement = "X" if context_text[0] != "X" else "Y"
    context["context_text"] = replacement + context_text[1:]
    _rewrite_artifact(
        destination, metadata_manifest, metadata_batches
    )
    with pytest.raises(ValueError, match="prompt metadata differs"):
        validate_input_manifest(
            destination / "manifest.json",
            destination / "chunks",
            stage=LINE_ADJUDICATION_STAGE,
            contract=contract,
        )


def test_round3_a2_real_restart_binds_frozen_prompt_token_ledger(
    tmp_path: Path,
):
    destination = tmp_path / "round3-a2-frozen"
    build_recall40_round3_line_adjudication_export(
        DEFAULT_FULL_EXPORT_DIR,
        ROUND3_STAGE_A_EXPORT,
        ROUND3_STAGE_A_OUTPUT,
        destination,
    )
    contract = computed_recall40_round3_stage_a2_contract()
    _manifest, manifest_hash, input_paths = validate_input_manifest(
        destination / "manifest.json",
        destination / "chunks",
        stage=LINE_ADJUDICATION_STAGE,
        contract=contract,
    )
    assert len(input_paths) == 1
    batches = read_gzip_jsonl(input_paths[0])
    prompt_counts = [5970, 23058, 7739]
    assert len(batches) == len(prompt_counts)
    args = _args(batches[0])
    generation_hash = generation_config_sha256(
        args, contract, input_manifest_sha256=manifest_hash
    )
    rows = [
        _derive_output_row(
            batch=batch,
            raw_output=json.dumps(_valid_output(batch)),
            finish_reason="stop",
            stop_reason=None,
            output_tokens=64,
            prompt_tokens=prompt_tokens,
            args=args,
            contract=contract,
            input_manifest_sha256=manifest_hash,
            generation_hash=generation_hash,
        )
        for batch, prompt_tokens in zip(batches, prompt_counts)
    ]
    output_path = tmp_path / "chunk_00000.out.jsonl.gz"
    _write_gzip(output_path, rows)
    replay = {
        "stage": LINE_ADJUDICATION_STAGE,
        "input_manifest_sha256": manifest_hash,
        "expected_generation_config_sha256": generation_hash,
        "expected_max_model_len": 32768,
        "expected_max_output_tokens": DEFAULT_MAX_OUTPUT_TOKENS[
            LINE_ADJUDICATION_STAGE
        ],
        "expected_prompt_token_counts_sha256": sha256_json(prompt_counts),
        "contract_override": contract,
    }
    assert line_ledger_chunk_is_complete(
        input_paths[0], output_path, **replay
    )

    tampered = copy.deepcopy(rows)
    tampered[1]["prompt_tokens_used"] += 1
    tampered[1]["context_margin_tokens"] -= 1
    _write_gzip(output_path, tampered)
    assert not line_ledger_chunk_is_complete(
        input_paths[0], output_path, **replay
    )
    replay.pop("expected_prompt_token_counts_sha256")
    _write_gzip(output_path, rows)
    assert not line_ledger_chunk_is_complete(
        input_paths[0], output_path, **replay
    )


def test_historical_stage_a2_contract_and_spec_are_unchanged():
    assert LINE_ADJUDICATION_SPEC_SHA256 == (
        "ea397860de9e5f543aabdad071269c780dba9a49fe03441d4f7c9b61168e2c5a"
    )
    assert get_frozen_stage_a2_v4_contract()[
        "line_ledger_contract_sha256"
    ] == FROZEN_STAGE_A2_V4_CONTRACT_SHA256
