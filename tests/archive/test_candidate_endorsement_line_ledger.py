from __future__ import annotations

import copy
import gzip
import hashlib
import json
from collections import Counter, defaultdict
from pathlib import Path

import pytest

from src.candidate_endorsement_line_ledger import (
    ANCHOR_ROLES,
    BASE_BIOGRAPHY_PROMPT_SHA256,
    BASE_BIOGRAPHY_SCHEMA_SHA256,
    BASE_ISSUE_PROMPT_SHA256,
    BASE_ISSUE_SCHEMA_SHA256,
    LINE_BATCH_SPEC_SHA256,
    LINE_ADJUDICATION_SCHEMAS,
    LINE_ADJUDICATION_SLOT_COUNT,
    LINE_ADJUDICATION_SPEC_SHA256,
    LINE_DECISION_SCHEMA,
    LINE_DECISION_SLOT_COUNT,
    LINE_INVENTORY_SPEC_SHA256,
    MODEL_RECORD_REPETITION_THRESHOLD,
    RECORDS_PER_SLOT_GUIDED_MAXIMUM,
    RECORD_EXTRACTION_SCHEMA,
    SCHEMA_CATALOG_SHA256,
    SCHEMA_VARIANTS,
    SCHEMA_VARIANT_SHA256,
    RECORD_EXTRACTION_SLOT_COUNT,
    RECALL40_ROUND3_LINE_DECISION_PROMPT,
    RECALL40_ROUND3_STAGE_A_SEMANTIC_REVISION_SPEC_SHA256,
    RECALL40_ROUND3_STAGE_A_CONTEXT_PROFILE_SHA256,
    RECALL40_ROUND3_STAGE_A_LINE_BATCH_SPEC_SHA256,
    align_record_extraction_output,
    build_line_adjudication_batches,
    build_line_decision_batches,
    build_record_extraction_batches,
    build_record_extraction_batches_from_adjudicated_merge,
    build_stage_user_message,
    candidate_name_aliases,
    computed_recall40_round3_stage_a_contract,
    expected_batch_counts,
    get_line_ledger_contract,
    inventory_census,
    line_adjudication_exception_ledger,
    line_adjudication_ledger,
    line_decision_ledger,
    merge_line_adjudication_ledger,
    reconstruct_parent_inventory,
    prompt_for_contract_stage,
    schema_binding_for_batch,
    standalone_machine_json,
    sha256_json,
    sha256_text,
    validate_line_decision_output,
    validate_round3_stage_a_context_batch,
    validate_line_adjudication_output,
    validate_record_extraction_output,
    whole_line_candidate_self_eligible,
    _physical_lines,
    _line_adjudication_item_schema,
    _compound_anchor_expected_names,
    _name_is_maximal_delimited_mention,
    _projected_record_evidence,
    _projected_support_mode,
    _required_split_line_name,
    _record_block,
)
from src.candidate_extraction import (
    build_endorsement_context_provenance,
    get_extraction_contract,
)


REPO = Path(__file__).resolve().parents[1]
FULL_EXPORT = (
    REPO / "build/hive_candidate_extraction/v2_1_full_export_20260824p"
)


def test_record_projection_uses_nearest_duplicate_value_and_exact_end_boundary() -> None:
    parent_text = "Alice\nMayor\nMayor\n"
    target = {
        "source_line_id": "a" * 64,
        "record_block_lines": [
            {"source_line_id": "a" * 64, "line_text": "Alice", "parent_start_char": 0, "parent_end_char": 5},
            {"source_line_id": "b" * 64, "line_text": "Mayor", "parent_start_char": 6, "parent_end_char": 11},
            {"source_line_id": "c" * 64, "line_text": "Mayor", "parent_start_char": 12, "parent_end_char": 17},
        ],
    }
    assert _projected_record_evidence(parent_text, target, {
        "endorser_name": "Alice", "title": "Mayor",
        "affiliation": None, "jurisdiction": None,
    }) == ("Alice\nMayor", 0, 11)


def test_split_line_and_shared_surname_names_are_fully_accounted() -> None:
    assert _projected_support_mode(
        "The Houston Educational Support Personnel Union 6315",
        {"endorser_name": "The Houston Educational Support Personnel Union 6315",
         "title": None, "affiliation": None, "jurisdiction": None},
    ) == "page_or_section_context"
    assert _projected_support_mode(
        "Alice Smith endorses Jamie Doe",
        {"endorser_name": "Alice Smith", "title": None,
         "affiliation": None, "jurisdiction": None},
    ) == "explicit_text"
    for source, truncated in (
        ("Houston Councilmember Tiffany D. Thomas", "Thomas"),
        ("Neil T. Freeman", "Freeman"),
        ("Wayne A. Derby", "Derby"),
        ("Ken Park, Jr.", "Jr."),
        ("Julie A. Casimiro", "Casimiro"),
        ("Frank S. Lombardi", "Lombardi"),
        ("J. Deeb", "Deeb"),
    ):
        assert not _name_is_maximal_delimited_mention(source, truncated)
    target = {
        "source_line_id": "a" * 64,
        "line_text": "State Senator, Elizabeth",
        "record_block_lines": [
            {"source_line_id": "a" * 64, "line_text": "State Senator, Elizabeth", "parent_start_char": 0, "parent_end_char": 25},
            {"source_line_id": "b" * 64, "line_text": "Steiner Hayward", "parent_start_char": 26, "parent_end_char": 41},
        ],
    }
    assert _required_split_line_name(target) == "Elizabeth Steiner Hayward"
    assert _projected_record_evidence(
        "State Senator, Elizabeth\nSteiner Hayward", target, {
            "endorser_name": "Elizabeth Steiner Hayward",
            "title": "State Senator", "affiliation": None, "jurisdiction": None,
        }
    ) == ("State Senator, Elizabeth\nSteiner Hayward", 0, 41)
    for anchor, expected in {
        "Brian and Christine Ivinjack": ("Brian Ivinjack", "Christine Ivinjack"),
        "Becky and Dana Ryl": ("Becky Ryl", "Dana Ryl"),
        "Tom and Lisa Ganucheau": ("Tom Ganucheau", "Lisa Ganucheau"),
        "John & Dee Simmons": ("John Simmons", "Dee Simmons"),
        "Bend-La Pine Schools Board Members: Shimiko Montgomery, Melissa Barnes-Dholakia and Carrie Douglass": (
            "Shimiko Montgomery", "Melissa Barnes-Dholakia", "Carrie Douglass",
        ),
        "Working Families Party & Oregon Working Families Party": (
            "Working Families Party", "Oregon Working Families Party",
        ),
    }.items():
        assert _compound_anchor_expected_names(anchor) == expected


def test_general_stage_a2_candidate_and_machine_json_eligibility() -> None:
    assert candidate_name_aliases("Tanaka, Greg Lin") == (
        "greg lin tanaka", "greg tanaka",
    )
    assert whole_line_candidate_self_eligible(
        "  GREG\u00a0TANAKA  ", "Tanaka, Greg Lin"
    )
    assert not whole_line_candidate_self_eligible(
        "Greg Tanaka For Congress 2024", "Tanaka, Greg Lin"
    )
    assert not whole_line_candidate_self_eligible(
        "Addressing the climate crisis.", "Smith, Jamie"
    )
    assert standalone_machine_json('{"items": ["a", "b"]}')
    assert standalone_machine_json('[1, 2, 3]')
    assert not standalone_machine_json('prefix {"items": []}')
    assert not standalone_machine_json('{not valid JSON}')
    assert not standalone_machine_json('[NaN]')
    assert not standalone_machine_json('{"x": Infinity}')
    assert not standalone_machine_json('[-Infinity]')
    unowned_json_schema = _line_adjudication_item_schema(
        target=True,
        exception_reason="stage_a_unowned_continuation",
        allowed_anchor_source_line_ids=["a" * 64],
        terminal_roles=["nonrecord_content"],
    )
    assert len(unowned_json_schema["oneOf"]) == 1
    assert unowned_json_schema["oneOf"][0]["properties"]["decision"][
        "enum"
    ] == ["role"]


def test_long_continuation_component_keeps_one_shared_distant_owner() -> None:
    lines = []
    decisions = {}
    roles = ["record_anchor"] + ["record_continuation_next"] * 5
    for index, role in enumerate(roles):
        line_id = hashlib.sha256(f"line-{index}".encode()).hexdigest()
        text = f"line {index}"
        lines.append({
            "source_line_id": line_id,
            "parent_line_index": index,
            "line_text": text,
            "line_text_sha256": hashlib.sha256(text.encode()).hexdigest(),
            "model_decision_required": True,
        })
        decisions[line_id] = {
            "source_line_id": line_id,
            "role": role,
            "stage_a_batch_id": f"batch-{index}",
            "stage_a_slot": "slot_00",
        }
    parent = {
        "parent_document_id": "parent-long-component",
        "candidate_icpsr": "Smith, Jamie",
        "lines": lines,
    }
    ledger = line_adjudication_exception_ledger([parent], decisions)
    owner_id = lines[0]["source_line_id"]
    assert len(ledger) == 5
    assert all(
        row["allowed_anchor_source_line_ids"] == [owner_id]
        for row in ledger
    )
    assert ledger[-1]["visible_lines"][0]["source_line_id"] == owner_id


def test_unowned_machine_json_exception_is_forced_nonrecord_end_to_end() -> None:
    rows = _segments()
    old = "Mayor of Example"
    replacement = '{"x":[1,2,3]   }'
    assert len(old) == len(replacement)
    full_text = (
        "Endorsements\nAlice Smith\n" + replacement
        + "\nBob Jones and Carol Lee\n"
    )
    descriptors = [{
        "document_id": row["document_id"],
        "segment_start_char": row["segment_start_char"],
        "segment_end_char": row["segment_end_char"],
    } for row in rows]
    for row in rows:
        start = int(row["segment_start_char"])
        end = int(row["segment_end_char"])
        row["extraction_source_text_sha256"] = sha256_text(full_text)
        row["source_text"] = full_text[start:end]
        row["segment_text_sha256"] = sha256_text(row["source_text"])
        row["endorsement_context_provenance"] = (
            build_endorsement_context_provenance(
                parent_document_id=str(row["parent_document_id"]),
                parent_source_text=full_text,
                source_urls=row["source_urls"],
                segment_id=str(row["document_id"]),
                segment_start_char=start,
                segment_end_char=end,
                parent_segments=descriptors,
            )
        )
    parent = reconstruct_parent_inventory(rows)
    model_lines = [
        line for line in parent["lines"] if line["model_decision_required"]
    ]
    roles = ["unclear", "record_continuation_next", "nonrecord_content"]
    decisions = {
        str(line["source_line_id"]): {
            "source_line_id": str(line["source_line_id"]),
            "role": role,
            "stage_a_batch_id": "json-stage-a",
            "stage_a_slot": f"slot_{index:02d}",
        }
        for index, (line, role) in enumerate(zip(model_lines, roles, strict=True))
    }
    provenance_payload = {
        "stage_a_input_manifest_sha256": "a" * 64,
        "stage_a_line_ledger_contract_sha256": (
            "ac8d6ed7f06e58a7961e08f8d50f7a86907f1bada12f1c70f5e9963011a335c2"
        ),
        "stage_a_generation_config_sha256": "b" * 64,
        "stage_a_ordered_output_batch_ids_sha256": "c" * 64,
        "stage_a_output_files": [],
        "by_batch_id": {"json-stage-a": {
            "input_chunk_file": "in.gz", "input_chunk_sha256": "d" * 64,
            "output_chunk_file": "out.gz", "output_chunk_sha256": "e" * 64,
            "input_batch_sha256": "f" * 64, "raw_output_sha256": "1" * 64,
            "model_output_sha256": "2" * 64,
            "generation_config_sha256": "b" * 64,
        }},
    }
    provenance = {**provenance_payload, "stage_a_provenance_sha256": sha256_json(
        provenance_payload
    )}
    batch = build_line_adjudication_batches(
        [parent], decisions, stage_a_provenance=provenance
    )[0]
    target = next(
        value for value in batch["targets"]
        if value["stage_a_decision"]["role"] == "record_continuation_next"
    )
    assert target["deterministic_eligibility"]["standalone_machine_json"] is True
    assert target["allowed_anchor_source_line_ids"] == []
    schema = schema_binding_for_batch(batch)["schema"]["properties"][
        target["slot"]
    ]
    assert [
        branch["properties"]["decision"]["enum"][0]
        for branch in schema["oneOf"]
    ] == ["role"]
    output = {}
    for value in batch["targets"]:
        output[value["slot"]] = {
            "decision": "role",
            "final_role": (
                "nonrecord_content"
                if value["deterministic_eligibility"]["standalone_machine_json"]
                else "record_anchor"
            ),
            "anchor_source_line_id": None,
        }
    for slot in batch["padding_slots"]:
        output[slot] = {
            "decision": "padding", "final_role": None,
            "anchor_source_line_id": None,
        }
    assert validate_line_adjudication_output(batch, output)["schema_valid"]
    adjudications = line_adjudication_ledger([
        batch
    ], [{"batch_id": batch["batch_id"], "model_output": output}])
    merged = merge_line_adjudication_ledger([parent], decisions, adjudications)
    assert merged["validation"]["validation_passed"] is True
    assert merged["final_decisions"][target["source_line_id"]]["role"] == (
        "nonrecord_content"
    )


def _context(
    *,
    parent_id: str,
    segment_id: str,
    segment_start: int,
    segment_end: int,
    first_complete_parent: int,
    last_complete_parent: int,
    first_complete_local: int,
    last_complete_local: int,
) -> dict[str, object]:
    return {
        "context_id": hashlib.sha256(segment_id.encode()).hexdigest(),
        "parent_document_id": parent_id,
        "source_scope": "parent_document",
        "context_text": "Endorsements",
        "context_text_sha256": sha256_text("Endorsements"),
        "context_parent_start_char": 0,
        "context_parent_end_char": 12,
        "context_scope_parent_start_char": 13,
        "context_scope_parent_end_char": 66,
        "context_source_segment_id": segment_id,
        "context_source_segment_start_char": segment_start,
        "context_source_segment_end_char": segment_end,
        "context_local_start_char": None,
        "context_local_end_char": None,
        "context_derivation_method": "test_parent_context",
        "qualifying_wayback_url": None,
        "qualifying_original_url": None,
        "qualifying_original_path": None,
        "url_classifier_id": "src.extract_text.classify_page_type_v1",
        "url_classifier_spec_sha256": "classifier",
        "segment_has_complete_line": True,
        "segment_first_complete_line_parent_start_char": first_complete_parent,
        "segment_last_complete_line_parent_end_char": last_complete_parent,
        "segment_first_complete_line_start_char": first_complete_local,
        "segment_last_complete_line_end_char": last_complete_local,
    }


def _segments() -> list[dict[str, object]]:
    parent = (
        "Endorsements\n"
        "Alice Smith\n"
        "Mayor of Example\n"
        "Bob Jones and Carol Lee\n"
    )
    parent_id = "parent"
    bounds = [(0, 45), (26, len(parent))]
    segment_ids = [
        hashlib.sha256(f"segment-{index}".encode()).hexdigest()
        for index in range(len(bounds))
    ]
    descriptors = [
        {
            "document_id": segment_ids[index],
            "segment_start_char": start,
            "segment_end_char": end,
        }
        for index, (start, end) in enumerate(bounds)
    ]
    source_urls = [
        "https://web.archive.org/web/20240101000000/https://example.com/endorsements/"
    ]
    rows = []
    for index, (start, end) in enumerate(bounds):
        text = parent[start:end]
        segment_id = segment_ids[index]
        rows.append({
            "candidate_cycle_id": "H4TX00000-2024",
            "candidate_icpsr": "Candidate, Test",
            "document_id": segment_id,
            "parent_document_id": parent_id,
            "route": "endorsements",
            "state": "TX",
            "office": "house",
            "year": 2024,
            "party": "democrat",
            "raw_text_sha256": "raw",
            "extraction_source_text_sha256": sha256_text(parent),
            "extraction_source_n_chars": len(parent),
            "source_urls": source_urls,
            "segment_index": index,
            "segment_count": 2,
            "segment_start_char": start,
            "segment_end_char": end,
            "segment_text_sha256": sha256_text(text),
            "source_text": text,
            "endorsement_context_provenance": build_endorsement_context_provenance(
                parent_document_id=parent_id,
                parent_source_text=parent,
                source_urls=source_urls,
                segment_id=segment_id,
                segment_start_char=start,
                segment_end_char=end,
                parent_segments=descriptors,
            ),
        })
    return rows


def _stage_a_output(batch: dict[str, object]) -> dict[str, object]:
    roles = ["record_anchor", "record_continuation_previous", "compound_record_anchor"]
    output = {}
    for index in range(LINE_DECISION_SLOT_COUNT):
        slot = f"slot_{index:02d}"
        output[slot] = {
            "role": roles[index] if slot in batch["target_slots"] else "padding"
        }
    return output


def test_frozen_issue_and_biography_contracts_are_unchanged() -> None:
    base = get_extraction_contract("2.1")
    assert BASE_ISSUE_PROMPT_SHA256 == base["prompt_sha256"]["issues"] == (
        "e7ad149cbbdfa5657b32235c3421adc4f5ca89fa564f7075e6a26e26f00fcb19"
    )
    assert BASE_ISSUE_SCHEMA_SHA256 == base["json_schema_sha256"]["issues"] == (
        "c5b9922e9447cc5c573bd3eeefa432710a0657e37c484c46e30dc0e83a41c719"
    )
    assert BASE_BIOGRAPHY_PROMPT_SHA256 == base["prompt_sha256"]["biography"] == (
        "d32cf8c87062d607cc1ee09e9485b90f2cc1a581559944583dc4839efa177960"
    )
    assert BASE_BIOGRAPHY_SCHEMA_SHA256 == base["json_schema_sha256"]["biography"] == (
        "ee197f260e3e2242d65ac05644d6cba72c2c2e3114fe127c579be6070ab3ffd7"
    )
    contract = get_line_ledger_contract()
    assert contract["line_inventory_spec_sha256"] == LINE_INVENTORY_SPEC_SHA256
    assert contract["line_batch_spec_sha256"] == LINE_BATCH_SPEC_SHA256


def test_round3_stage_a_contract_is_stage_specific_and_prompt_bound() -> None:
    contract = computed_recall40_round3_stage_a_contract()
    assert contract["active_stage"] == "line_decision"
    assert set(contract["prompt_sha256"]) == {"line_decision"}
    assert set(contract["json_schema_catalog_sha256"]) == {"line_decision"}
    assert set(contract["json_schema_variant_sha256"]) == {"line_decision"}
    assert "frozen_stage_a3_authentication" not in contract
    assert "stage_b_three_pass_provenance_spec_sha256" not in contract
    assert contract["recall40_round3_stage_a_semantic_revision_spec_sha256"] == (
        RECALL40_ROUND3_STAGE_A_SEMANTIC_REVISION_SPEC_SHA256
    )
    assert contract["stage_a_context_profile_sha256"] == (
        RECALL40_ROUND3_STAGE_A_CONTEXT_PROFILE_SHA256
    )
    assert contract["line_batch_spec_sha256"] == (
        RECALL40_ROUND3_STAGE_A_LINE_BATCH_SPEC_SHA256
    )
    assert prompt_for_contract_stage(contract, "line_decision") == (
        RECALL40_ROUND3_LINE_DECISION_PROMPT
    )
    with pytest.raises(ValueError, match="active stage"):
        prompt_for_contract_stage(contract, "record_extraction")


def test_round3_stage_a_batches_carry_exact_complete_parent_context() -> None:
    parent = reconstruct_parent_inventory(_segments())
    contract = computed_recall40_round3_stage_a_contract()
    batch = build_line_decision_batches(
        [parent], contract_override=contract,
    )[0]
    assert batch["visible_lines"] == parent["lines"]
    assert batch["full_parent_line_count"] == len(parent["lines"])
    assert batch["line_batch_spec_sha256"] == (
        RECALL40_ROUND3_STAGE_A_LINE_BATCH_SPEC_SHA256
    )
    validate_round3_stage_a_context_batch(batch)
    message = build_stage_user_message(batch)
    assert "complete authenticated parent" in message
    assert "Bob Jones and Carol Lee" in message

    attacked = copy.deepcopy(batch)
    attacked["visible_lines"][0]["line_text"] = "Injected"
    with pytest.raises(ValueError, match="catalog digest"):
        validate_round3_stage_a_context_batch(attacked)


def test_parent_reconstruction_inventory_and_fixed_stage_a_slots() -> None:
    parent = reconstruct_parent_inventory(_segments())
    assert parent["parent_source_text"].startswith("Endorsements\nAlice")
    census = inventory_census([parent])
    assert census["physical_lines"] == 4
    assert census["model_decision_lines"] == 3
    assert census["outside_scope_lines"] == 0
    assert census["context_anchor_boundary_lines"] == 1
    assert census["unique_source_line_ids"] == 4
    batches = build_line_decision_batches([parent])
    assert len(batches) == 1
    batch = batches[0]
    assert batch["target_slots"] == ["slot_00", "slot_01", "slot_02"]
    assert len(batch["padding_slots"]) == LINE_DECISION_SLOT_COUNT - 3
    assert set(LINE_DECISION_SCHEMA["required"]) == {
        f"slot_{index:02d}" for index in range(LINE_DECISION_SLOT_COUNT)
    }
    message = build_stage_user_message(batch)
    assert "TARGET slot_00" in message
    assert "CONTEXT" in message
    assert "Candidate ID: Candidate, Test" in message
    binding = schema_binding_for_batch(batch)
    assert batch["json_schema_variant_id"] == binding["json_schema_variant_id"]
    assert batch["json_schema_sha256"] == binding["json_schema_sha256"]


def test_target_count_schema_variants_structurally_forbid_target_padding() -> None:
    for stage, slot_count in (
        ("line_decision", LINE_DECISION_SLOT_COUNT),
        ("record_extraction", RECORD_EXTRACTION_SLOT_COUNT),
    ):
        assert len(SCHEMA_VARIANTS[stage]) == slot_count
        assert SCHEMA_CATALOG_SHA256[stage]
        for target_count in range(1, slot_count + 1):
            batch = {
                "stage": stage,
                "target_slots": [
                    f"slot_{index:02d}" for index in range(target_count)
                ],
                "padding_slots": [
                    f"slot_{index:02d}"
                    for index in range(target_count, slot_count)
                ],
            }
            if stage == "record_extraction":
                    batch["targets"] = [{
                        "slot": f"slot_{index:02d}",
                        "stage_a_role": "record_anchor",
                        "source_line_id": f"{index:064x}",
                    } for index in range(target_count)]
            binding = schema_binding_for_batch(batch)
            schema = binding["schema"]
            if stage == "record_extraction":
                assert binding["json_schema_sha256"] == sha256_json(schema)
            else:
                assert binding["json_schema_sha256"] == SCHEMA_VARIANT_SHA256[
                    stage
                ][binding["json_schema_variant_id"]]
            for index in range(slot_count):
                item = schema["properties"][f"slot_{index:02d}"]
                if stage == "record_extraction":
                    records_schema = item["properties"]["records"]
                    if index < target_count:
                        assert records_schema["minItems"] == 1
                    else:
                        assert records_schema["maxItems"] == 0
                    continue
                else:
                    enum = item["properties"]["role"]["enum"]
                if index < target_count:
                    assert "padding" not in enum
                else:
                    assert enum == ["padding"]


def test_stage_a_rejects_missing_padding_and_target_padding() -> None:
    batch = build_line_decision_batches([reconstruct_parent_inventory(_segments())])[0]
    output = _stage_a_output(batch)
    assert validate_line_decision_output(batch, output)["coverage_complete"]
    broken = dict(output)
    broken.pop("slot_15")
    assert not validate_line_decision_output(batch, broken)["schema_valid"]
    broken = json.loads(json.dumps(output))
    broken["slot_00"] = {"role": "padding"}
    assert "slot_00:target_marked_padding" in validate_line_decision_output(
        batch, broken
    )["errors"]


def test_stage_b_is_exactly_one_decision_per_anchor_and_aligns_records() -> None:
    parent = reconstruct_parent_inventory(_segments())
    stage_a = build_line_decision_batches([parent])
    outputs = [{
        "batch_id": stage_a[0]["batch_id"],
        "model_output": _stage_a_output(stage_a[0]),
    }]
    ledger = line_decision_ledger(stage_a, outputs)
    assert Counter(row["role"] for row in ledger.values()) == {
        "record_anchor": 1,
        "record_continuation_previous": 1,
        "compound_record_anchor": 1,
    }
    assert set(row["role"] for row in ledger.values()) & set(ANCHOR_ROLES)
    batches = build_record_extraction_batches([parent], ledger)
    assert len(batches) == 1
    batch = batches[0]
    assert len(batch["target_slots"]) == 2
    binding = schema_binding_for_batch(batch)
    assert binding["json_schema_variant_id"].startswith(
        "record_extraction_anchor_cardinality_"
    )
    assert batch["json_schema_sha256"] == binding["json_schema_sha256"]
    missing_target = json.loads(json.dumps(batch))
    missing_target["targets"].pop()
    with pytest.raises(ValueError, match="declared slot ledger"):
        schema_binding_for_batch(missing_target)
    reordered_targets = json.loads(json.dumps(batch))
    reordered_targets["targets"].reverse()
    with pytest.raises(ValueError, match="declared slot ledger"):
        schema_binding_for_batch(reordered_targets)
    duplicate_target = json.loads(json.dumps(batch))
    duplicate_target["targets"][1]["source_line_id"] = duplicate_target["targets"][0][
        "source_line_id"
    ]
    with pytest.raises(ValueError, match="declared slot ledger"):
        schema_binding_for_batch(duplicate_target)
    output: dict[str, object] = {}
    records = [
        {
            "endorser_name": "Alice Smith",
            "entity_type": "individual",
            "endorser_category": "private_individual",
                "title": "Mayor of Example",
                "affiliation": None,
                "jurisdiction": None,
        }
    ]
    for index in range(RECORD_EXTRACTION_SLOT_COUNT):
        slot = f"slot_{index:02d}"
        output[slot] = (
            {
                "records": records if index == 0 else [
                    {
                        **records[0],
                            "endorser_name": "Bob Jones",
                            "title": None,
                    },
                    {
                        **records[0],
                            "endorser_name": "Carol Lee",
                            "title": None,
                    },
                ],
            }
            if slot in batch["target_slots"]
            else {
                "records": [],
            }
        )
    validation = validate_record_extraction_output(batch, output)
    assert validation["schema_valid"]
    aligned = align_record_extraction_output(batch, output)
    assert aligned["aligned_record_count"] == 3
    assert aligned["withheld_record_count"] == 0
    assert aligned["public_coverage_complete"]
    assert all(item["record_evidence_carrier_segment_id"] for item in aligned["aligned_records"])
    assert len({
        item["source_model_item_id"] for item in aligned["aligned_records"]
    }) == 3
    assert all(
        item["carrier_alignment_source_model_item_id"]
        for item in aligned["aligned_records"]
    )
    assert set(RECORD_EXTRACTION_SCHEMA["required"]) == {
        f"slot_{index:02d}" for index in range(RECORD_EXTRACTION_SLOT_COUNT)
    }


def test_stage_b_target_cannot_be_empty_or_withheld() -> None:
    parent = reconstruct_parent_inventory(_segments())
    stage_a = build_line_decision_batches([parent])
    ledger = line_decision_ledger(stage_a, [{
        "batch_id": stage_a[0]["batch_id"],
        "model_output": _stage_a_output(stage_a[0]),
    }])
    batch = build_record_extraction_batches([parent], ledger)[0]
    output = {}
    for index in range(RECORD_EXTRACTION_SLOT_COUNT):
        slot = f"slot_{index:02d}"
        output[slot] = (
            {"records": []}
            if slot in batch["target_slots"]
            else {"records": []}
        )
    validation = validate_record_extraction_output(batch, output)
    assert not validation["schema_valid"]
    assert any("record_count_mismatch" in error for error in validation["errors"])


def _model_record(name: str, evidence: str | None = None) -> dict[str, object]:
    return {
        "endorser_name": name,
        "entity_type": "individual",
        "endorser_category": "private_individual",
        "title": None,
        "affiliation": None,
        "jurisdiction": None,
    }


def _stage_b_fixture() -> tuple[dict[str, object], dict[str, object]]:
    parent = reconstruct_parent_inventory(_segments())
    stage_a = build_line_decision_batches([parent])
    ledger = line_decision_ledger(stage_a, [{
        "batch_id": stage_a[0]["batch_id"],
        "model_output": _stage_a_output(stage_a[0]),
    }])
    batch = build_record_extraction_batches([parent], ledger)[0]
    output: dict[str, object] = {}
    for index in range(RECORD_EXTRACTION_SLOT_COUNT):
        slot = f"slot_{index:02d}"
        if slot == "slot_00":
            output[slot] = {
                "records": [_model_record("Alice Smith")],
            }
        elif slot == "slot_01":
            output[slot] = {
                "records": [
                    _model_record("Bob Jones"), _model_record("Carol Lee")
                ],
            }
        else:
            output[slot] = {
                "records": [],
            }
    return batch, output


def test_stage_b_cardinality_cap_and_repetition_are_fail_closed() -> None:
    batch, output = _stage_b_fixture()
    assert validate_record_extraction_output(batch, output)["schema_valid"]
    forbidden_reclassification = json.loads(json.dumps(output))
    forbidden_reclassification["slot_00"] = {
        "disposition": "not_endorsement",
        "records": [],
    }
    assert not validate_record_extraction_output(
        batch, forbidden_reclassification
    )["schema_valid"]

    truncated_ordinary = json.loads(json.dumps(output))
    truncated_ordinary["slot_00"]["records"][0]["endorser_name"] = "Alice"
    assert "slot_00:record_0:name_not_exact_in_block" in (
        validate_record_extraction_output(batch, truncated_ordinary)["errors"]
    )

    titled_batch = json.loads(json.dumps(batch))
    titled_output = json.loads(json.dumps(output))
    titled_target = titled_batch["targets"][0]
    titled_target["line_text"] = "Houston Councilmember Carolyn Evans Shabazz"
    titled_target["record_block_lines"] = [{
        "source_line_id": titled_target["source_line_id"],
        "line_text": titled_target["line_text"],
        "parent_start_char": 0, "parent_end_char": 42, "parent_full_end_char": 42,
    }]
    titled_batch["parent_source_text"] = titled_target["line_text"]
    titled_output["slot_00"]["records"][0].update({
        "endorser_name": "Carolyn Evans Shabazz",
        "title": "Houston Councilmember",
    })
    assert validate_record_extraction_output(titled_batch, titled_output)[
        "schema_valid"
    ]
    assert _name_is_maximal_delimited_mention(
        "U.S. Senator, Elizabeth Warren", "Elizabeth Warren",
        accounted_prefixes=("U.S. Senator", None),
    )
    assert _name_is_maximal_delimited_mention(
        "Vice Chair, DPO Environmental Caucus, Doyle Canning", "Doyle Canning",
        accounted_prefixes=("Vice Chair", "DPO Environmental Caucus"),
    )

    split_batch = json.loads(json.dumps(batch))
    split_output = json.loads(json.dumps(output))
    split_target = split_batch["targets"][0]
    split_target["line_text"] = "State Senator, Elizabeth"
    split_target["record_block_lines"] = [
        {"source_line_id": split_target["source_line_id"], "line_text": "State Senator, Elizabeth", "parent_start_char": 0, "parent_end_char": 25, "parent_full_end_char": 26},
        {"source_line_id": "f" * 64, "line_text": "Steiner Hayward", "parent_start_char": 26, "parent_end_char": 41, "parent_full_end_char": 41},
    ]
    split_batch["parent_source_text"] = "State Senator, Elizabeth\nSteiner Hayward"
    split_output["slot_00"]["records"][0].update({
        "endorser_name": "Elizabeth", "title": "State Senator",
    })
    split_report = validate_record_extraction_output(split_batch, split_output)
    assert "slot_00:split_line_name_not_fully_accounted" in split_report["errors"]
    split_output["slot_00"]["records"][0]["endorser_name"] = (
        "Elizabeth Steiner Hayward"
    )
    assert validate_record_extraction_output(split_batch, split_output)["schema_valid"]

    ordinary_comma_batch = json.loads(json.dumps(batch))
    ordinary_comma_output = json.loads(json.dumps(output))
    ordinary_target = ordinary_comma_batch["targets"][0]
    ordinary_target["line_text"] = "Rob, Independent Voter"
    ordinary_target["record_block_lines"] = [{
        "source_line_id": ordinary_target["source_line_id"],
        "line_text": "Rob, Independent Voter",
        "parent_start_char": 0,
        "parent_end_char": 22,
        "parent_full_end_char": 22,
    }]
    ordinary_comma_batch["parent_source_text"] = "Rob, Independent Voter"
    ordinary_comma_output["slot_00"]["records"][0]["endorser_name"] = "Rob Voter"
    report = validate_record_extraction_output(
        ordinary_comma_batch, ordinary_comma_output
    )
    assert "slot_00:record_0:name_not_exact_in_block" in report["errors"]
    with pytest.raises(ValueError, match="endorser_name is absent"):
        _projected_record_evidence(
            "Rob, Independent Voter", ordinary_target,
            ordinary_comma_output["slot_00"]["records"][0],
        )

    category_mismatch = json.loads(json.dumps(output))
    category_mismatch["slot_00"]["records"][0]["entity_type"] = "organization"
    assert "slot_00:record_0:invalid_model_schema" in validate_record_extraction_output(
        batch, category_mismatch
    )["errors"]
    record_branch = RECORD_EXTRACTION_SCHEMA["properties"]["slot_00"]
    assert record_branch["properties"]["records"]["maxItems"] == (
        RECORDS_PER_SLOT_GUIDED_MAXIMUM
    )

    wrong_single = json.loads(json.dumps(output))
    wrong_single["slot_00"]["records"].append(_model_record("Carol Lee"))
    report = validate_record_extraction_output(batch, wrong_single)
    assert "slot_00:single_anchor_record_count_mismatch" in report["errors"]

    wrong_compound = json.loads(json.dumps(output))
    wrong_compound["slot_01"]["records"] = [_model_record("Bob Jones")]
    report = validate_record_extraction_output(batch, wrong_compound)
    assert "slot_01:compound_anchor_record_count_mismatch" in report["errors"]

    combined_compound = json.loads(json.dumps(output))
    combined_compound["slot_01"]["records"][0]["endorser_name"] = "Bob Jones and Carol Lee"
    report = validate_record_extraction_output(batch, combined_compound)
    assert "slot_01:compound_anchor_combined_identity_name" in report["errors"]

    at_cap = json.loads(json.dumps(output))
    at_cap["slot_01"]["records"] = [
        _model_record(f"Person {index}")
        for index in range(RECORDS_PER_SLOT_GUIDED_MAXIMUM)
    ]
    report = validate_record_extraction_output(batch, at_cap)
    assert not report["records_per_slot_cap_reached"]
    assert not report["schema_valid"]

    over_cap = json.loads(json.dumps(at_cap))
    over_cap["slot_01"]["records"].append(_model_record("Person overflow"))
    assert validate_record_extraction_output(batch, over_cap)[
        "records_per_slot_cap_reached"
    ]

    repeated = json.loads(json.dumps(output))
    repeated["slot_01"]["records"] = [
        _model_record("Bob Jones"),
        _model_record("Carol Lee"),
        _model_record("Bob Jones"),
        _model_record("Carol Lee"),
        _model_record("Bob Jones"),
    ]
    report = validate_record_extraction_output(batch, repeated)
    assert report["max_exact_model_record_multiplicity"] == (
        MODEL_RECORD_REPETITION_THRESHOLD
    )
    assert "model_record_exact_repetition_threshold_reached" in report["errors"]

    double_allowed = json.loads(json.dumps(output))
    double_allowed["slot_01"]["records"] = [
        _model_record("Bob Jones"), _model_record("Bob Jones")
    ]
    report = validate_record_extraction_output(batch, double_allowed)
    assert report["max_exact_model_record_multiplicity"] == 2
    assert "model_record_exact_repetition_threshold_reached" not in report["errors"]


def test_directional_continuation_cannot_be_orphaned_or_shared() -> None:
    parent = reconstruct_parent_inventory(_segments())
    stage_a = build_line_decision_batches([parent])
    model_output = _stage_a_output(stage_a[0])
    model_output["slot_00"] = {"role": "record_continuation_next"}
    model_output["slot_01"] = {"role": "nonrecord_content"}
    ledger = line_decision_ledger(stage_a, [{
        "batch_id": stage_a[0]["batch_id"], "model_output": model_output,
    }])
    with pytest.raises(ValueError, match="directional continuation ownership"):
        build_record_extraction_batches([parent], ledger)


def test_physical_lines_split_only_lf_and_preserve_unicode_separators() -> None:
    rows = _physical_lines("alpha\u2028beta\rchar\vfield\nlast")
    assert [row["line_text"] for row in rows] == [
        "alpha\u2028beta\rchar\vfield", "last"
    ]
    assert [row["line_terminator"] for row in rows] == ["\n", ""]
    assert rows[0]["parent_full_end_char"] == len("alpha\u2028beta\rchar\vfield\n")


def test_overlong_record_block_fails_whole_export_before_inference() -> None:
    anchor_id = sha256_text("anchor")
    continuation_id = sha256_text("continuation")
    parent = {
        "lines": [
            {
                "source_line_id": anchor_id,
                "parent_line_index": 0,
                "line_text": "Anchor",
            },
            {
                "source_line_id": continuation_id,
                "parent_line_index": 1,
                "line_text": "x" * 8_192,
            },
        ]
    }
    decisions = {
        anchor_id: {"role": "record_anchor"},
        continuation_id: {"role": "record_continuation_previous"},
    }
    with pytest.raises(ValueError, match="record block exceeds"):
        _record_block(parent, parent["lines"][0], decisions)


def _adjudication_fixture() -> tuple[
    list[dict[str, object]], dict[str, dict[str, object]], dict[str, object]
]:
    parent = reconstruct_parent_inventory(_segments())
    targets = [line for line in parent["lines"] if line["model_decision_required"]]
    roles = ("unclear", "record_continuation_previous", "compound_record_anchor")
    decisions = {
        str(line["source_line_id"]): {
            "source_line_id": str(line["source_line_id"]),
            "role": role,
            "stage_a_batch_id": "stage-a-batch",
            "stage_a_slot": f"slot_{index:02d}",
        }
        for index, (line, role) in enumerate(zip(targets, roles))
    }
    provenance_payload = {
        "stage_a_input_manifest_sha256": "a" * 64,
        "stage_a_line_ledger_contract_sha256": (
            "ac8d6ed7f06e58a7961e08f8d50f7a86907f1bada12f1c70f5e9963011a335c2"
        ),
        "stage_a_generation_config_sha256": "b" * 64,
        "stage_a_ordered_output_batch_ids_sha256": "c" * 64,
        "stage_a_output_files": [],
        "by_batch_id": {
            "stage-a-batch": {
                "input_chunk_file": "chunk.jsonl.gz",
                "input_chunk_sha256": "d" * 64,
                "output_chunk_file": "chunk.out.jsonl.gz",
                "output_chunk_sha256": "e" * 64,
                "input_batch_sha256": "f" * 64,
                "raw_output_sha256": "1" * 64,
                "model_output_sha256": "2" * 64,
                "generation_config_sha256": "b" * 64,
            }
        },
    }
    provenance = {
        **provenance_payload,
        "stage_a_provenance_sha256": sha256_json(provenance_payload),
    }
    return [parent], decisions, provenance


def _adjudication_output(batch: dict[str, object]) -> dict[str, object]:
    output = {}
    for index in range(LINE_ADJUDICATION_SLOT_COUNT):
        slot = f"slot_{index:02d}"
        if index >= len(batch["target_slots"]):
            output[slot] = {
                "decision": "padding",
                "final_role": None,
                "anchor_source_line_id": None,
            }
            continue
        target = batch["targets"][index]
        if target["exception_reason"] == "stage_a_unclear":
            output[slot] = {
                "decision": "role",
                "final_role": "record_anchor",
                "anchor_source_line_id": None,
            }
        else:
            output[slot] = {
                "decision": "attach_to_anchor",
                "final_role": None,
                "anchor_source_line_id": target[
                    "allowed_anchor_source_line_ids"
                ][0],
            }
    return output


def test_line_adjudication_exact_exception_merge_and_provenance() -> None:
    parents, decisions, provenance = _adjudication_fixture()
    exceptions = line_adjudication_exception_ledger(parents, decisions)
    assert [row["exception_reason"] for row in exceptions] == [
        "stage_a_unclear", "stage_a_unowned_continuation",
    ]
    batches = build_line_adjudication_batches(
        parents, decisions, stage_a_provenance=provenance
    )
    assert len(batches) == 1
    assert batches[0]["line_adjudication_spec_sha256"] == (
        LINE_ADJUDICATION_SPEC_SHA256
    )
    output = _adjudication_output(batches[0])
    validation = validate_line_adjudication_output(batches[0], output)
    assert validation["schema_valid"] is True
    rows = [{"batch_id": batches[0]["batch_id"], "model_output": output}]
    adjudications = line_adjudication_ledger(batches, rows)
    merged = merge_line_adjudication_ledger(parents, decisions, adjudications)
    assert merged["validation"] == {
        "expected_exception_count": 2,
        "adjudicated_exception_count": 2,
        "unresolved_line_count": 0,
        "withheld_line_count": 0,
        "unowned_continuation_count": 0,
        "duplicate_continuation_owner_count": 0,
        "ownership_conflict_count": 0,
        "coverage_complete": True,
        "validation_passed": True,
    }
    continuation_id = exceptions[1]["source_line_id"]
    anchor_id = next(
        line_id for line_id, decision in decisions.items()
        if decision["role"] == "compound_record_anchor"
    )
    assert merged["ownership_by_continuation"][continuation_id] == anchor_id
    assert merged["per_line_provenance"][continuation_id][
        "stage_a_proposal"
    ]["role"] == "record_continuation_previous"
    assert merged["per_line_provenance"][continuation_id][
        "stage_a2_adjudication"
    ]["stage_a2_decision"]["decision"] == "attach_to_anchor"
    assert merged["per_line_provenance"][continuation_id][
        "stage_a2_adjudication"
    ]["stage_a2_output_provenance_locator"]["model_output_sha256"] == (
        sha256_json(output)
    )
    stage_b = build_record_extraction_batches(
        parents,
        merged["final_decisions"],
        per_line_provenance=merged["per_line_provenance"],
        adjudication_pass_count=2,
    )
    assert len(stage_b) == 1
    target = stage_b[0]["targets"][0]
    assert target["stage_a_proposed_role"] == "unclear"
    assert target["stage_a_role"] == "record_anchor"
    assert target["line_adjudication_applied"] is True
    assert target["stage_a2_decision"]["decision"] == "role"
    assert target["anchor_line_decision_provenance"] == merged[
        "per_line_provenance"
    ][target["source_line_id"]]
    owner_target = next(
        row for batch in stage_b for row in batch["targets"]
        if row["source_line_id"] == anchor_id
    )
    assert {
        row["source_line_id"]
        for row in owner_target["record_block_line_decision_provenance"]
    } == {anchor_id, continuation_id}
    assert stage_b[0]["final_line_decisions_sha256"] == merged[
        "final_decisions_sha256"
    ]
    compound_slot = next(
        target["slot"] for target in stage_b[0]["targets"]
        if target["stage_a_role"] == "compound_record_anchor"
    )
    compound_schema = schema_binding_for_batch(stage_b[0])["schema"][
        "properties"
    ][compound_slot]
    assert compound_schema["properties"]["records"]["minItems"] == 2
    assert stage_b[0]["two_pass_line_provenance_sha256"] == merged[
        "per_line_provenance_sha256"
    ]
    replayed_stage_b = build_record_extraction_batches_from_adjudicated_merge(
        parents, merged
    )
    assert [sha256_json(row) for row in replayed_stage_b] == [
        sha256_json(row) for row in stage_b
    ]
    tampered_merge = json.loads(json.dumps(merged))
    tampered_merge["per_line_provenance"][anchor_id]["final_role"] = (
        "nonrecord_content"
    )
    with pytest.raises(ValueError, match="not valid for Stage B"):
        build_record_extraction_batches_from_adjudicated_merge(
            parents, tampered_merge
        )
    with pytest.raises(ValueError, match="does not cover"):
        build_record_extraction_batches(
            parents,
            merged["final_decisions"],
            per_line_provenance={anchor_id: merged["per_line_provenance"][anchor_id]},
            adjudication_pass_count=2,
        )


def test_line_adjudication_fails_closed_on_disallowed_or_withheld_decision() -> None:
    parents, decisions, provenance = _adjudication_fixture()
    batch = build_line_adjudication_batches(
        parents, decisions, stage_a_provenance=provenance
    )[0]
    invalid = _adjudication_output(batch)
    invalid["slot_01"]["anchor_source_line_id"] = "not-an-allowed-anchor"
    report = validate_line_adjudication_output(batch, invalid)
    assert report["schema_valid"] is False
    assert "slot_01:invalid_anchor_attachment" in report["errors"]

    withheld = _adjudication_output(batch)
    withheld["slot_00"] = {
        "decision": "withheld_validation",
        "final_role": None,
        "anchor_source_line_id": None,
    }
    adjudications = line_adjudication_ledger(
        [batch], [{"batch_id": batch["batch_id"], "model_output": withheld}]
    )
    merged = merge_line_adjudication_ledger(parents, decisions, adjudications)
    assert merged["validation"]["withheld_line_count"] == 1
    assert merged["validation"]["ownership_conflict_count"] == 0
    assert merged["validation"]["validation_passed"] is False


def test_line_adjudication_schema_variants_exclude_real_slot_padding() -> None:
    assert set(LINE_ADJUDICATION_SCHEMAS) == set(
        range(1, LINE_ADJUDICATION_SLOT_COUNT + 1)
    )
    for target_count, schema in LINE_ADJUDICATION_SCHEMAS.items():
        for index in range(LINE_ADJUDICATION_SLOT_COUNT):
            item = schema["properties"][f"slot_{index:02d}"]
            encoded = json.dumps(item, sort_keys=True)
            if index < target_count:
                assert '"padding"' not in encoded
                assert '"withheld_validation"' in encoded
            else:
                assert item["properties"]["decision"]["enum"] == ["padding"]

    def walk(value: object):
        if isinstance(value, dict):
            yield value
            for child in value.values():
                yield from walk(child)
        elif isinstance(value, list):
            for child in value:
                yield from walk(child)

    for schema in LINE_ADJUDICATION_SCHEMAS.values():
        for node in walk(schema):
            assert not ({"$ref", "allOf", "const", "pattern", "minLength"} & set(node))


def test_no_carrier_is_privately_withheld_without_unbound_state() -> None:
    batch, output = _stage_b_fixture()
    broken = json.loads(json.dumps(batch))
    for descriptor in broken["segment_descriptors"]:
        descriptor["segment_start_char"] = 0
        descriptor["segment_end_char"] = 1
        descriptor["source_text"] = broken["parent_source_text"][:1]
    aligned = align_record_extraction_output(broken, output)
    assert aligned["aligned_record_count"] == 0
    assert aligned["withheld_record_count"] == 3
    assert {
        item["reason"] for item in aligned["withheld_records"]
    } == {"record_evidence_has_no_single_source_segment_carrier"}


@pytest.mark.skipif(not FULL_EXPORT.exists(), reason="frozen full export unavailable")
def test_full_endorsement_source_replay_has_frozen_line_census() -> None:
    grouped: dict[str, list[dict[str, object]]] = defaultdict(list)
    for path in sorted((FULL_EXPORT / "chunks").glob("chunk_*.jsonl.gz")):
        with gzip.open(path, "rt", encoding="utf-8") as handle:
            for line in handle:
                row = json.loads(line)
                if row["route"] == "endorsements":
                    grouped[row["parent_document_id"]].append(row)
    parents = [
        reconstruct_parent_inventory(rows) for _, rows in sorted(grouped.items())
    ]
    assert inventory_census(parents) == {
        "parent_documents": 2583,
        "source_segments": 6096,
        "physical_lines": 307819,
        "nonempty_physical_lines": 307503,
        "empty_physical_lines": 316,
        "model_decision_lines": 286704,
        "model_decision_line_characters": 12439357,
        "outside_scope_lines": 19300,
        "context_anchor_boundary_lines": 1499,
        "reconstructed_long_lines": 39,
        "complete_line_overlap_duplicate_occurrences": 20106,
        "unique_source_line_ids": 307819,
        "unique_model_decision_line_ids": 286704,
    }
    assert expected_batch_counts(parents) == {
        "line_decision_batches": 19142,
        "record_extraction_batches_maximum": 36995,
    }
    batches = build_line_decision_batches(parents)
    assert Counter(len(batch["target_slots"]) for batch in batches) == {
        1: 179, 2: 178, 3: 191, 4: 159, 5: 130, 6: 154, 7: 140,
        8: 158, 9: 161, 10: 163, 11: 161, 12: 166, 13: 165,
        14: 139, 15: 159, 16: 16_739,
    }
    for batch in batches:
        binding = schema_binding_for_batch(batch)
        assert batch["json_schema_variant_id"] == binding[
            "json_schema_variant_id"
        ]
        assert batch["json_schema_sha256"] == binding["json_schema_sha256"]
