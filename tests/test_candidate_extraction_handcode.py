from __future__ import annotations

import json
from copy import deepcopy
from pathlib import Path

import pytest

from scripts.build_candidate_extraction_handcode import (
    BIOGRAPHY_FIELDS_V2,
    DEFAULT_SEED,
    HandcodeInputError,
    build_parent_pages,
    build_assignment_batches,
    build_task_html,
    build_task_payloads,
    canonical_json,
    handcode_renderer_provenance,
    precision_task,
    precision_pool_observed_levels,
    select_precision_records,
    select_recall_pages,
    sha256_file,
    sha256_text,
    validate_bundle_generation_config,
)
from scripts.score_candidate_extraction_handcode import (
    HumanValidationError,
    finalize_passing_bundle,
    score_handcode_results,
    verify_handcode_selection_against_bundle,
)
from scripts.hive.extract_candidate_pages_vllm import (
    RAW_STRUCTURAL_WHITESPACE_VALIDATOR_ID,
    RAW_STRUCTURAL_WHITESPACE_VALIDATOR_SPEC_SHA256,
    STRUCTURED_DECODING_BACKEND,
    STRUCTURED_DECODING_DISABLE_ANY_WHITESPACE,
    build_user_message,
    canonicalize_json_string_controls,
    validate_raw_structural_whitespace,
)
from scripts.import_candidate_extraction_v2 import (
    EXPECTED_GENERATION_CONFIG,
    EXPECTED_GENERATION_CONFIG_SHA256,
    EXPECTED_MAX_OUTPUT_TOKENS,
    EXPECTED_STRUCTURED_DECODING,
    all_output_aggregate,
    build_record_variants,
    output_provenance_summary,
    parent_offsets,
    resolve_parent_variants,
)
from src.candidate_extraction import (
    MODEL_ID,
    MODEL_REVISION,
    RUNTIME_LOCK_SHA256,
    align_extraction_exact_spans,
    build_endorsement_context_provenance,
    get_extraction_contract,
    inference_display_provenance,
    pre_alignment_model_item_guard_report,
    validate_extraction,
)
from src.candidate_endorsement_line_ledger import get_line_ledger_contract


ISSUE_KINDS = (
    "support",
    "oppose",
    "proposal",
    "past_action",
    "current_action",
    "unclear",
)
ENDORSEMENT_CATEGORIES = (
    "private_individual",
    "government_official",
    "party_official",
    "labor_union",
    "media_outlet",
    "other_organization",
    "other_individual",
    "unclear",
)


def _page(
    route: str,
    index: int,
    records: list[dict],
    *,
    segment_count: int = 1,
    length: int = 400,
) -> dict:
    parent = sha256_text(f"parent-{route}-{index}")
    candidate = f"H{index:08d}-2020"
    page = {
        "parent_document_id": parent,
        "candidate_cycle_id": candidate,
        "candidate_icpsr": f"Candidate {index}",
        "state": f"S{index % 5}",
        "office": "house",
        "year": 2000 + 2 * (index % 12),
        "party": ("democrat", "republican", "other")[index % 3],
        "route": route,
        "raw_text_sha256": sha256_text(f"raw-{route}-{index}"),
        "source_text": "Source text " + ("x" * length),
        "segment_count": segment_count,
        "records": [],
    }
    for record_index, model in enumerate(records):
        subtype_field = {
            "biography": "field",
            "issues": "statement_kind",
            "endorsements": "endorser_category",
        }[route]
        subtype = model[subtype_field]
        page["records"].append({
            "record_id": sha256_text(f"record-{route}-{index}-{record_index}"),
            "parent_document_id": parent,
            "route": route,
            "subtype": subtype,
            "model_record": model,
            "source_segment_ids": [sha256_text(f"segment-{route}-{index}")],
            "highlights": [],
        })
    return page


def _selection_pages() -> list[dict]:
    pages: list[dict] = []
    index = 0
    for field in BIOGRAPHY_FIELDS_V2:
        for _ in range(35):
            pages.append(_page(
                "biography",
                index,
                [{"field": field, "value": f"Value {index}"}],
                segment_count=2 if index % 2 == 0 else 1,
            ))
            index += 1
    for zero_index in range(15):
        pages.append(_page("biography", index + zero_index, []))
    index += 15

    for issue_index in range(180):
        kind = ISSUE_KINDS[issue_index % len(ISSUE_KINDS)]
        pages.append(_page(
            "issues",
            index + issue_index,
            [{
                "statement_kind": kind,
                "claim": f"Claim {issue_index}",
                "source_heading": None if issue_index % 2 else "Heading",
            }],
            segment_count=2 if issue_index % 2 == 0 else 1,
            length=(1_000, 5_000, 10_000)[issue_index % 3],
        ))
    index += 180
    for zero_index in range(25):
        pages.append(_page("issues", index + zero_index, []))
    index += 25

    entity = {
        "private_individual": "individual",
        "government_official": "individual",
        "party_official": "individual",
        "labor_union": "organization",
        "media_outlet": "organization",
        "other_organization": "organization",
        "other_individual": "individual",
        "unclear": "unclear",
    }
    for endorsement_index in range(210):
        category = ENDORSEMENT_CATEGORIES[endorsement_index % len(ENDORSEMENT_CATEGORIES)]
        optional = endorsement_index < 75
        pages.append(_page(
            "endorsements",
            index + endorsement_index,
            [{
                "endorser_name": f"Endorser {endorsement_index}",
                "entity_type": entity[category],
                "endorser_category": category,
                "title": "Mayor" if optional else None,
                "affiliation": "Group" if optional else None,
                "jurisdiction": "AA" if optional else None,
                "support_mode": (
                    "explicit_text" if endorsement_index % 2 else "page_or_section_context"
                ),
                "endorsement_context_evidence_source_scope": (
                    "segment",
                    "parent_document",
                    "page_or_section_route_context",
                )[endorsement_index % 3],
            }],
            segment_count=2 if endorsement_index % 2 == 0 else 1,
        ))
    index += 210
    for zero_index in range(15):
        pages.append(_page("endorsements", index + zero_index, []))
    return pages


def test_declared_samples_and_batches_are_deterministic() -> None:
    pages = _selection_pages()
    excluded = {pages[0]["parent_document_id"]}
    precision = select_precision_records(pages, excluded, DEFAULT_SEED)
    recall = select_recall_pages(pages, excluded, DEFAULT_SEED)
    second = select_precision_records(pages, excluded, DEFAULT_SEED)

    assert [row["record_id"] for row in precision["issues"]] == [
        row["record_id"] for row in second["issues"]
    ]
    assert len(precision["biography"]) == 60
    biography_counts = {}
    for row in precision["biography"]:
        biography_counts[row["subtype"]] = biography_counts.get(row["subtype"], 0) + 1
    assert biography_counts == {field: 10 for field in BIOGRAPHY_FIELDS_V2}
    assert len(precision["issues"]) == 60
    assert {row["subtype"] for row in precision["issues"]} == set(ISSUE_KINDS)
    assert len(precision["endorsements"]) == 60
    endorsement_records = precision["endorsements"]
    assert {row["model_record"]["endorser_category"] for row in endorsement_records} == set(
        ENDORSEMENT_CATEGORIES
    )
    assert {
        row["model_record"]["endorsement_context_evidence_source_scope"]
        for row in endorsement_records
    } == {"segment", "parent_document", "page_or_section_route_context"}
    for field in ("title", "affiliation", "jurisdiction"):
        assert sum(bool(row["model_record"].get(field)) for row in endorsement_records) >= 20

    for route, route_pages in recall.items():
        assert len(route_pages) == 20
        assert sum(not page["records"] for page in route_pages) >= 5
        assert sum(page["segment_count"] > 1 for page in route_pages) >= 5
        assert not {page["parent_document_id"] for page in route_pages} & excluded

    tasks = build_task_payloads(precision, recall, "a" * 64, batch_size=60)
    assert len(tasks) == 6
    assert sum(len(task["payload"]["items"]) for task in tasks) == 240
    assert len({task["task_id"] for task in tasks}) == 6


def _single_enriched_segment(*, strict: bool = False) -> dict:
    source = "Jobs\nWe support good jobs."
    evidence = "We support good jobs."
    evidence_start = source.index(evidence)
    item = {
        "source_heading": "Jobs",
        "source_heading_start_char": 0,
        "source_heading_end_char": 4,
        "claim": "The candidate supports good jobs.",
        "statement_kind": "support",
        "evidence": evidence,
        "evidence_start_char": evidence_start,
        "evidence_end_char": evidence_start + len(evidence),
    }
    model_item = {
        key: value
        for key, value in item.items()
        if not key.endswith("_start_char") and not key.endswith("_end_char")
    }
    extraction, withheld, alignment = align_extraction_exact_spans(
        "issues", {"issue_blocks": [model_item]}, source
    )
    assert extraction is not None
    assert withheld == []
    assert alignment["status"] == "complete"
    item = extraction["issue_blocks"][0]
    candidate = "H0AA00001-2020"
    route = "issues"
    raw_hash = sha256_text(source)
    parent = sha256_text(f"{candidate}\n{route}\n{raw_hash}")
    segment_hash = sha256_text(source)
    segment = sha256_text(f"{parent}\n0\n0\n{len(source)}\n{segment_hash}")
    row = {
        "document_id": segment,
        "parent_document_id": parent,
        "candidate_cycle_id": candidate,
        "candidate_icpsr": "Candidate One",
        "state": "AA",
        "office": "house",
        "year": 2020,
        "party": "democrat",
        "route": route,
        "raw_text_sha256": raw_hash,
        "source_text": source,
        "extraction_source_text_sha256": sha256_text(source),
        "segment_index": 0,
        "segment_count": 1,
        "segment_start_char": 0,
        "segment_end_char": len(source),
        "segment_text_sha256": segment_hash,
        "parse_status": "parsed",
        "processing_status": "processed_success",
        "validation": validate_extraction(route, extraction, source, "2.1"),
        "input_truncated": False,
        "finish_reason": "stop",
        "extraction": extraction,
        "resolved_records": [{
            "record_id": sha256_text("record"),
            "parent_document_id": parent,
            "route": route,
            "canonical_segment_id": segment,
            "all_source_segment_ids": [segment],
            "resolution_status": "single_variant",
            "item": item,
            "parent_offsets": {
                "source_heading_parent_start_char": 0,
                "source_heading_parent_end_char": 4,
                "evidence_parent_start_char": evidence_start,
                "evidence_parent_end_char": evidence_start + len(evidence),
            },
        }],
    }
    if not strict:
        return row

    contract = get_extraction_contract("2.1")
    model_extraction = {"issue_blocks": [model_item]}
    guard = pre_alignment_model_item_guard_report(route, model_extraction)
    raw_output = json.dumps(model_extraction, ensure_ascii=False)
    _, output_report = canonicalize_json_string_controls(raw_output)
    display_provenance = inference_display_provenance(source)
    structural_whitespace = validate_raw_structural_whitespace(raw_output)
    row.update({
        "model_id": MODEL_ID,
        "model_revision": MODEL_REVISION,
        "runtime_lock_sha256": RUNTIME_LOCK_SHA256,
        "schema_version": "2.1",
        "prompt_sha256": contract["prompt_sha256"][route],
        "json_schema_sha256": contract["json_schema_sha256"][route],
        "extraction_contract_sha256": contract[
            "extraction_contract_sha256"
        ],
        "alignment_id": contract["alignment_id"],
        "alignment_spec_sha256": contract["alignment_spec_sha256"],
        "semantic_validation_spec_sha256": contract[
            "semantic_validation_spec_sha256"
        ],
        "endorsement_context_provenance_spec_sha256": contract[
            "endorsement_context_provenance_spec_sha256"
        ],
        "user_message_template_id": contract["user_message_template_id"],
        "user_message_template_sha256": contract[
            "user_message_template_sha256"
        ],
        **display_provenance,
        "raw_output": raw_output,
        **output_report,
        **structural_whitespace,
        "array_item_cap_spec_id": contract["array_item_cap_spec_id"],
        "array_item_cap_spec_sha256": contract["array_item_cap_spec_sha256"],
        "route_array_item_cap": contract["route_array_item_caps"][route],
        "model_item_repetition_spec_id": contract[
            "model_item_repetition_spec_id"
        ],
        "model_item_repetition_spec_sha256": contract[
            "model_item_repetition_spec_sha256"
        ],
        "generation_config_sha256": EXPECTED_GENERATION_CONFIG_SHA256,
        "structured_decoding_backend": STRUCTURED_DECODING_BACKEND,
        "structured_decoding_disable_any_whitespace": (
            STRUCTURED_DECODING_DISABLE_ANY_WHITESPACE
        ),
        "model_extraction": model_extraction,
        "model_extraction_sha256": alignment[
            "original_model_extraction_sha256"
        ],
        "pre_alignment_model_item_guard": guard,
        "array_item_cap_report": guard["array_item_cap_report"],
        "model_item_repetition_report": guard["model_item_repetition_report"],
        "aligned_extraction_sha256": alignment[
            "aligned_extraction_sha256"
        ],
        "withheld_model_items": withheld,
        "exact_alignment": alignment,
        "input_chars_used": len(source),
        "input_tokens_used": 10,
        "output_tokens": 100,
        "max_output_tokens": EXPECTED_MAX_OUTPUT_TOKENS,
        "stop_reason": None,
        "temperature": 0.0,
        "thinking_enabled": False,
        "compact_repair": False,
        "record_variants": [],
        "resolved_records": [],
    })
    row["user_message_sha256"] = sha256_text(
        build_user_message(row, source, "2.1")
    )
    variants = build_record_variants([row])
    annotated, resolved = resolve_parent_variants(parent, route, variants)
    row["record_variants"] = annotated
    row["resolved_records"] = resolved
    return row


def _strict_manifest(rows: list[dict]) -> dict:
    contract = get_extraction_contract("2.1")
    resolved = sum(len(row["resolved_records"]) for row in rows)
    conflicts = {
        variant["overlap_group_id"]
        for row in rows
        for variant in row["record_variants"]
        if variant["overlap_resolution_status"] == "conflict_withheld"
    }
    return {
        "extraction_contract_sha256": contract[
            "extraction_contract_sha256"
        ],
        "model_id": MODEL_ID,
        "model_revision": MODEL_REVISION,
        "runtime_lock_sha256": RUNTIME_LOCK_SHA256,
        "prompt_sha256": contract["prompt_sha256"],
        "json_schema_sha256": contract["json_schema_sha256"],
        "endorsement_context_provenance_spec_sha256": contract[
            "endorsement_context_provenance_spec_sha256"
        ],
        "user_message_template_id": contract[
            "user_message_template_id"
        ],
        "user_message_template_sha256": contract[
            "user_message_template_sha256"
        ],
        "inference_display_escape_id": contract[
            "inference_display_escape_id"
        ],
        "inference_display_escape_spec_sha256": contract[
            "inference_display_escape_spec_sha256"
        ],
        "json_string_control_canonicalizer_id": contract[
            "json_string_control_canonicalizer_id"
        ],
        "json_string_control_canonicalizer_spec_sha256": contract[
            "json_string_control_canonicalizer_spec_sha256"
        ],
        "array_item_cap_spec_id": contract["array_item_cap_spec_id"],
        "array_item_cap_spec_sha256": contract["array_item_cap_spec_sha256"],
        "route_array_item_caps": contract["route_array_item_caps"],
        "model_item_repetition_spec_id": contract[
            "model_item_repetition_spec_id"
        ],
        "model_item_repetition_spec_sha256": contract[
            "model_item_repetition_spec_sha256"
        ],
        "generation_config": {
            "structured_decoding": EXPECTED_STRUCTURED_DECODING,
        },
        "generation_config_sha256": EXPECTED_GENERATION_CONFIG_SHA256,
        "structured_decoding_backend": STRUCTURED_DECODING_BACKEND,
        "structured_decoding_disable_any_whitespace": (
            STRUCTURED_DECODING_DISABLE_ANY_WHITESPACE
        ),
        "private_output_provenance_summary": output_provenance_summary(rows),
        "all_output_aggregate": all_output_aggregate(rows),
        "resolved_records": resolved,
        "withheld_overlap_conflict_groups": len(conflicts),
    }


def test_enriched_bundle_uses_importer_resolved_records() -> None:
    row = _single_enriched_segment()
    pages, checks = build_parent_pages([row], {"resolved_records": 1})
    assert checks["passed"] is True
    assert checks["records"] == 1
    assert pages[0]["records"][0]["record_id"] == sha256_text("record")
    assert pages[0]["source_text"] == row["source_text"]


def test_strict_handcode_replays_raw_output_alignment_and_resolution() -> None:
    row = _single_enriched_segment(strict=True)
    manifest = _strict_manifest([row])
    pages, checks = build_parent_pages([row], manifest)
    assert checks["passed"] is True
    assert checks["silently_selected_overlap_conflicts"] == 0
    assert len(pages[0]["records"]) == 1

    tampered_alignment = deepcopy(row)
    tampered_alignment["extraction"]["issue_blocks"][0]["claim"] = (
        "A backfilled claim"
    )
    with pytest.raises(
        HandcodeInputError, match="extraction differs from raw-output replay"
    ):
        build_parent_pages([tampered_alignment], manifest)

    tampered_resolution = deepcopy(row)
    tampered_resolution["resolved_records"][0]["resolution_method"] = (
        "first_variant_wins"
    )
    with pytest.raises(
        HandcodeInputError, match="resolved records differ from replay"
    ):
        build_parent_pages([tampered_resolution], manifest)

    weakened_policy = deepcopy(row)
    weakened_policy["structured_decoding_disable_any_whitespace"] = False
    with pytest.raises(HandcodeInputError, match="structured_decoding"):
        build_parent_pages([weakened_policy], manifest)

    string_policy = deepcopy(row)
    string_policy["structured_decoding_disable_any_whitespace"] = "True"
    with pytest.raises(HandcodeInputError, match="structured_decoding"):
        build_parent_pages([string_policy], manifest)

    guard_failure = deepcopy(row)
    guard_failure["parse_status"] = "model_item_repetition_error"
    guard_failure["processing_status"] = "failed_model_item_repetition_error"
    with pytest.raises(HandcodeInputError, match="did not process successfully"):
        build_parent_pages([guard_failure], manifest)


@pytest.mark.parametrize("blank", ["", " \t\n"])
def test_strict_handcode_rejects_blank_anchor_relabelled_as_success(
    blank: str,
) -> None:
    row = _single_enriched_segment(strict=True)
    model_extraction = deepcopy(row["model_extraction"])
    model_extraction["issue_blocks"][0]["evidence"] = blank
    aligned, withheld, alignment = align_extraction_exact_spans(
        "issues", model_extraction, row["source_text"]
    )
    assert alignment["status"] == "invalid_schema"
    assert alignment["aligned_item_count"] == 0

    raw_output = json.dumps(model_extraction, ensure_ascii=False)
    _, output_report = canonicalize_json_string_controls(raw_output)
    guard = pre_alignment_model_item_guard_report(
        "issues", model_extraction
    )
    row.update(output_report)
    row.update(validate_raw_structural_whitespace(raw_output))
    row.update({
        "raw_output": raw_output,
        "model_extraction": model_extraction,
        "model_extraction_sha256": alignment[
            "original_model_extraction_sha256"
        ],
        "pre_alignment_model_item_guard": guard,
        "array_item_cap_report": guard["array_item_cap_report"],
        "model_item_repetition_report": guard[
            "model_item_repetition_report"
        ],
        "extraction": aligned,
        "aligned_extraction_sha256": alignment[
            "aligned_extraction_sha256"
        ],
        "withheld_model_items": withheld,
        "exact_alignment": alignment,
        "validation": validate_extraction(
            "issues", aligned, row["source_text"], "2.1"
        ),
        # Simulate a tamper that tries to turn invalid model output into a
        # zero-record page eligible for recall handcoding.
        "processing_status": "processed_success",
        "record_variants": [],
        "resolved_records": [],
    })
    manifest = _strict_manifest([row])
    with pytest.raises(
        HandcodeInputError,
        match="processing status differs from raw-output replay",
    ):
        build_parent_pages([row], manifest)


def test_handcode_requires_complete_generation_config_hash() -> None:
    manifest = {
        "generation_config": deepcopy(EXPECTED_GENERATION_CONFIG),
        "generation_config_sha256": EXPECTED_GENERATION_CONFIG_SHA256,
        "array_item_cap_spec_id": get_extraction_contract("2.1")[
            "array_item_cap_spec_id"
        ],
        "array_item_cap_spec_sha256": get_extraction_contract("2.1")[
            "array_item_cap_spec_sha256"
        ],
        "route_array_item_caps": get_extraction_contract("2.1")[
            "route_array_item_caps"
        ],
        "model_item_repetition_spec_id": get_extraction_contract("2.1")[
            "model_item_repetition_spec_id"
        ],
        "model_item_repetition_spec_sha256": get_extraction_contract("2.1")[
            "model_item_repetition_spec_sha256"
        ],
        "array_item_cap_reached_outputs": 0,
        "model_item_repetition_error_outputs": 0,
        "pre_alignment_model_item_guard_gate_passed": True,
    }
    validate_bundle_generation_config(manifest)

    partial = deepcopy(manifest)
    partial["generation_config"] = {
        "structured_decoding": deepcopy(EXPECTED_STRUCTURED_DECODING)
    }
    with pytest.raises(HandcodeInputError, match="config hash differs"):
        validate_bundle_generation_config(partial)

    mutated = deepcopy(manifest)
    mutated["generation_config"]["seed"] += 1
    with pytest.raises(HandcodeInputError, match="config hash differs"):
        validate_bundle_generation_config(mutated)


def test_handcode_validates_endorsement_route_context_after_parent_rebuild() -> None:
    source = "Organizations\nAlice Example"
    candidate = "H0AA00002-2020"
    route = "endorsements"
    raw_hash = sha256_text(source)
    parent = sha256_text(f"{candidate}\n{route}\n{raw_hash}")
    segment_hash = sha256_text(source)
    segment = sha256_text(
        f"{parent}\n0\n0\n{len(source)}\n{segment_hash}"
    )
    source_urls = [
        "https://web.archive.org/web/20200101000000/"
        "https://example.org/supporters"
    ]
    context = build_endorsement_context_provenance(
        parent_document_id=parent,
        parent_source_text=source,
        source_urls=source_urls,
        segment_id=segment,
        segment_start_char=0,
        segment_end_char=len(source),
    )
    model = {"endorsements": [{
        "endorser_name": "Alice Example",
        "entity_type": "individual",
        "endorser_category": "private_individual",
        "title": None,
        "affiliation": None,
        "jurisdiction": None,
        "record_evidence": "Alice Example",
        "endorsement_context_evidence": None,
        "support_mode": "page_or_section_context",
    }]}
    extraction, withheld, alignment = align_extraction_exact_spans(
        route,
        model,
        source,
        endorsement_context_provenance=context,
        segment_start_char=0,
    )
    assert withheld == []
    item = extraction["endorsements"][0]
    record_id = sha256_text("endorsement-record")
    row = {
        "document_id": segment,
        "parent_document_id": parent,
        "candidate_cycle_id": candidate,
        "candidate_icpsr": "Candidate Two",
        "state": "AA",
        "office": "house",
        "year": 2020,
        "party": "democrat",
        "route": route,
        "raw_text_sha256": raw_hash,
        "source_text": source,
        "source_urls": source_urls,
        "endorsement_context_provenance": context,
        "extraction_source_text_sha256": sha256_text(source),
        "segment_index": 0,
        "segment_count": 1,
        "segment_start_char": 0,
        "segment_end_char": len(source),
        "segment_text_sha256": segment_hash,
        "parse_status": "parsed",
        "processing_status": "processed_success",
        "validation": validate_extraction(
            route,
            extraction,
            source,
            "2.1",
            endorsement_context_provenance=context,
            segment_start_char=0,
            parent_source_text=source,
        ),
        "input_truncated": False,
        "finish_reason": "stop",
        "extraction": extraction,
        "exact_alignment": alignment,
        "resolved_records": [{
            "record_id": record_id,
            "parent_document_id": parent,
            "route": route,
            "canonical_segment_id": segment,
            "all_source_segment_ids": [segment],
            "resolution_status": "single_variant",
            "item": item,
            "parent_offsets": parent_offsets(route, item, 0),
        }],
    }
    pages, checks = build_parent_pages([row], {"resolved_records": 1})
    assert checks["passed"] is True
    assert pages[0]["records"][0]["record_id"] == record_id


@pytest.mark.parametrize(
    ("processing_status", "keep_record"),
    [
        ("processed_partial_withheld", True),
        ("processed_all_withheld", False),
    ],
)
def test_handcode_pages_include_accounted_withheld_model_items(
    processing_status: str,
    keep_record: bool,
) -> None:
    row = _single_enriched_segment()
    row["processing_status"] = processing_status
    if not keep_record:
        row["extraction"] = {"issue_blocks": []}
        row["validation"] = validate_extraction(
            "issues", row["extraction"], row["source_text"], "2.1"
        )
        row["resolved_records"] = []
    pages, checks = build_parent_pages(
        [row], {"resolved_records": int(keep_record)}
    )
    assert len(pages) == 1
    assert len(pages[0]["records"]) == int(keep_record)
    assert checks["passed"] is True


def _scoring_fixture() -> tuple[dict, dict, dict, dict]:
    pages = _selection_pages()
    tuning_pages = [
        page for page in pages
        if page["route"] == "issues" and page["records"] and page["segment_count"] == 1
    ][:11]
    tuning_ids = {page["parent_document_id"] for page in tuning_pages}
    known_omissions = []
    for index, page in enumerate(tuning_pages):
        record = page["records"][0]
        known_omissions.append({
            "omission_id": sha256_text(f"known-omission-{index}"),
            "parent_document_id": page["parent_document_id"],
            "candidate_cycle_id": page["candidate_cycle_id"],
            "route": "issues",
            "expected_type": record["subtype"],
            "expected_value": record["model_record"]["claim"],
            "evidence": "Source text",
            "matched_record_ids": [record["record_id"]],
            "page": page,
            "matches": [record],
        })
    precision = select_precision_records(pages, tuning_ids, DEFAULT_SEED)
    pool_levels = precision_pool_observed_levels(pages, tuning_ids)
    precision_parents = {
        row["page"]["parent_document_id"]
        for route_rows in precision.values()
        for row in route_rows
    }
    recall = select_recall_pages(pages, tuning_ids | precision_parents, DEFAULT_SEED)
    tasks = build_task_payloads(
        precision, recall, "b" * 64, 60, known_omissions
    )
    task_payloads = {task["task_id"]: task["payload"] for task in tasks}
    manifest = {
        "format": "candidate-extraction-handcode-v2",
        "schema_version": "2.1",
        "seed": DEFAULT_SEED,
        "batch_size": 60,
        "bundle_manifest_sha256": "b" * 64,
        "segments_file": "segments.jsonl.gz",
        "segments_file_sha256": "c" * 64,
        "source_corpus_sha256": "d" * 64,
        "generation_config_sha256": EXPECTED_GENERATION_CONFIG_SHA256,
        "extraction_contract_sha256": get_extraction_contract("2.1")[
            "extraction_contract_sha256"
        ],
        "endorsement_context_provenance_spec_sha256": (
            get_extraction_contract("2.1")[
                "endorsement_context_provenance_spec_sha256"
            ]
        ),
        "endorsement_line_ledger_contract_sha256": get_line_ledger_contract()[
            "line_ledger_contract_sha256"
        ],
        "user_message_template_id": get_extraction_contract("2.1")[
            "user_message_template_id"
        ],
        "user_message_template_sha256": get_extraction_contract("2.1")[
            "user_message_template_sha256"
        ],
        "array_item_cap_spec_id": get_extraction_contract("2.1")[
            "array_item_cap_spec_id"
        ],
        "array_item_cap_spec_sha256": get_extraction_contract("2.1")[
            "array_item_cap_spec_sha256"
        ],
        "route_array_item_caps": get_extraction_contract("2.1")[
            "route_array_item_caps"
        ],
        "model_item_repetition_spec_id": get_extraction_contract("2.1")[
            "model_item_repetition_spec_id"
        ],
        "model_item_repetition_spec_sha256": get_extraction_contract("2.1")[
            "model_item_repetition_spec_sha256"
        ],
        "array_item_cap_reached_outputs": 0,
        "model_item_repetition_error_outputs": 0,
        "pre_alignment_model_item_guard_gate_passed": True,
        "structured_decoding_backend": STRUCTURED_DECODING_BACKEND,
        "structured_decoding_disable_any_whitespace": (
            STRUCTURED_DECODING_DISABLE_ANY_WHITESPACE
        ),
        "raw_structural_whitespace_validator_id": (
            RAW_STRUCTURAL_WHITESPACE_VALIDATOR_ID
        ),
        "raw_structural_whitespace_validator_spec_sha256": (
            RAW_STRUCTURAL_WHITESPACE_VALIDATOR_SPEC_SHA256
        ),
        "raw_structural_whitespace_gate_passed": True,
        "handcode_renderer": handcode_renderer_provenance(),
        "excluded_parent_ids": {
            "count": len(tuning_ids),
            "sha256": sha256_text(canonical_json(sorted(tuning_ids))),
            "values": sorted(tuning_ids),
        },
        "known_omission_recovery_file": {
            "file": "known_omission_recovery.json",
            "sha256": "f" * 64,
        },
        "technical_checks": {
            "passed": True,
            "schema_valid_rate": 1.0,
            "exact_span_rate": 1.0,
            "silently_selected_overlap_conflicts": 0,
            "withheld_overlap_conflicts": 0,
            "truncated_segments": 0,
            "user_message_hash_replay_rate": 1.0,
            "extraction_contract_sha256": get_extraction_contract("2.1")[
                "extraction_contract_sha256"
            ],
            "endorsement_context_provenance_spec_sha256": (
                get_extraction_contract("2.1")[
                    "endorsement_context_provenance_spec_sha256"
                ]
            ),
            "endorsement_line_ledger_contract_sha256": (
                get_line_ledger_contract()["line_ledger_contract_sha256"]
            ),
            "endorsement_line_ledger_public_coverage_complete": True,
            "user_message_template_id": get_extraction_contract("2.1")[
                "user_message_template_id"
            ],
            "user_message_template_sha256": get_extraction_contract("2.1")[
                "user_message_template_sha256"
            ],
            "raw_structural_whitespace_validator_id": (
                RAW_STRUCTURAL_WHITESPACE_VALIDATOR_ID
            ),
            "raw_structural_whitespace_validator_spec_sha256": (
                RAW_STRUCTURAL_WHITESPACE_VALIDATOR_SPEC_SHA256
            ),
            "raw_structural_whitespace_gate_passed": True,
            "array_item_cap_spec_id": get_extraction_contract("2.1")[
                "array_item_cap_spec_id"
            ],
            "array_item_cap_spec_sha256": get_extraction_contract("2.1")[
                "array_item_cap_spec_sha256"
            ],
            "route_array_item_caps": get_extraction_contract("2.1")[
                "route_array_item_caps"
            ],
            "model_item_repetition_spec_id": get_extraction_contract("2.1")[
                "model_item_repetition_spec_id"
            ],
            "model_item_repetition_spec_sha256": get_extraction_contract("2.1")[
                "model_item_repetition_spec_sha256"
            ],
            "array_item_cap_reached_outputs": 0,
            "model_item_repetition_error_outputs": 0,
            "pre_alignment_model_item_guard_gate_passed": True,
        },
        "selection": {"pool_observed_levels": pool_levels},
        "tasks": [
            {
                "task_id": task["task_id"],
                "kind": task["kind"],
                "route": task["route"],
                "batch_index": task["batch_index"],
                "item_count": len(task["payload"]["items"]),
                "sample_items": task["sample_items"],
            }
            for task in tasks
        ],
    }
    manifest["assignment_batches"] = build_assignment_batches(
        manifest["tasks"]
    )
    manifest["known_omission_tasks_separate_from_holdout"] = True
    results = {}
    hashes = {}
    for task in tasks:
        task_id = task["task_id"]
        responses = []
        for item in task["payload"]["items"]:
            answers = {
                question["id"]: "yes"
                for question in task["payload"]["questions"]
                if question["type"] == "yesno"
            }
            responses.append({
                "item_id": item["id"],
                "status": "complete",
                "answers": answers,
            })
        results[task_id] = {
            "task_id": task_id,
            "format": "handcode-results-v1",
            "exported_at": "2026-08-24T12:00:00Z",
            "n_items": len(responses),
            "n_complete": len(responses),
            "responses": responses,
        }
        hashes[task_id] = sha256_text(canonical_json(results[task_id]))
    return manifest, task_payloads, results, hashes


def _selection_replay_fixture(
    manifest: dict, task_payloads: dict, manifest_sha256: str = "e" * 64
) -> dict:
    return {
        "version": "candidate_extraction_handcode_selection_replay_v1",
        "passed": True,
        "bundle_manifest_sha256": manifest["bundle_manifest_sha256"],
        "handcode_manifest_sha256": manifest_sha256,
        "task_payloads_sha256": sha256_text(canonical_json({
            task_id: task_payloads[task_id] for task_id in sorted(task_payloads)
        })),
        "sample_inventory_sha256": sha256_text(canonical_json([
            {
                field: entry.get(field)
                for field in (
                    "task_id",
                    "kind",
                    "route",
                    "batch_index",
                    "item_count",
                    "sample_items",
                )
            }
            for entry in manifest["tasks"]
        ])),
        "handcode_renderer_sha256": sha256_text(
            canonical_json(manifest["handcode_renderer"])
        ),
        "selection_sha256": sha256_text(canonical_json(manifest["selection"])),
        "seed": manifest["seed"],
        "batch_size": manifest["batch_size"],
        "excluded_parent_ids_sha256": manifest["excluded_parent_ids"]["sha256"],
    }


def test_selection_replay_rejects_coordinated_task_substitution(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    manifest, task_payloads, _, _ = _scoring_fixture()
    entry = deepcopy(manifest["tasks"][0])
    task_id = entry["task_id"]
    task_payloads = {task_id: task_payloads[task_id]}
    manifest["tasks"] = [entry]
    expected_tasks = [
        {
            "task_id": entry["task_id"],
            "kind": entry["kind"],
            "route": entry["route"],
            "batch_index": entry["batch_index"],
            "payload": task_payloads[entry["task_id"]],
            "sample_items": entry["sample_items"],
        }
        for entry in manifest["tasks"]
    ]
    task_path = tmp_path / f"{task_id}.json"
    html_path = tmp_path / f"{task_id}.html"
    task_path.write_text(
        json.dumps(task_payloads[task_id], indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    build_task_html(task_path, html_path)
    entry["task_json"] = {
        "file": task_path.name,
        "sha256": sha256_file(task_path),
    }
    entry["html"] = {
        "file": html_path.name,
        "bytes": html_path.stat().st_size,
        "sha256": sha256_file(html_path),
    }
    prepared = {
        "bundle_info": {
            "bundle_manifest_sha256": manifest["bundle_manifest_sha256"],
            "segments_file": manifest["segments_file"],
            "segments_file_sha256": manifest["segments_file_sha256"],
            "bundle_manifest": {
                "source_corpus_sha256": manifest["source_corpus_sha256"],
            },
            "known_omission_recovery_file": manifest[
                "known_omission_recovery_file"
            ],
        },
        "selection": manifest["selection"],
        "tasks": expected_tasks,
    }
    monkeypatch.setattr(
        "scripts.score_candidate_extraction_handcode.prepare_handcode_selection",
        lambda *args, **kwargs: prepared,
    )
    monkeypatch.setattr(
        "scripts.score_candidate_extraction_handcode.build_assignment_batches",
        lambda _entries: manifest["assignment_batches"],
    )
    verified = verify_handcode_selection_against_bundle(
        tmp_path, tmp_path, manifest, "e" * 64, task_payloads
    )
    assert verified["passed"] is True

    html_tampered_manifest = deepcopy(manifest)
    html_path.write_text(
        html_path.read_text(encoding="utf-8")
        + "\n<p>Fabricated coding instruction</p>\n",
        encoding="utf-8",
    )
    html_tampered_manifest["tasks"][0]["html"] = {
        "file": html_path.name,
        "bytes": html_path.stat().st_size,
        "sha256": sha256_file(html_path),
    }
    with pytest.raises(
        HumanValidationError, match="rendered app differs from deterministic replay"
    ):
        verify_handcode_selection_against_bundle(
            tmp_path,
            tmp_path,
            html_tampered_manifest,
            "f" * 64,
            task_payloads,
        )

    build_task_html(task_path, html_path)

    substituted_manifest = deepcopy(manifest)
    substituted_tasks = deepcopy(task_payloads)
    substituted_display = next(
        display
        for display in substituted_tasks[task_id]["items"][0]["display"]
        if display.get("format") == "text"
    )
    substituted_display["content"] = "fabricated easier holdout text"
    task_path.write_text(
        json.dumps(substituted_tasks[task_id], indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    build_task_html(task_path, html_path)
    substituted_manifest["tasks"][0]["task_json"] = {
        "file": task_path.name,
        "sha256": sha256_file(task_path),
    }
    substituted_manifest["tasks"][0]["html"] = {
        "file": html_path.name,
        "bytes": html_path.stat().st_size,
        "sha256": sha256_file(html_path),
    }
    with pytest.raises(
        HumanValidationError, match="displayed task differs from deterministic replay"
    ):
        verify_handcode_selection_against_bundle(
            tmp_path,
            tmp_path,
            substituted_manifest,
            "0" * 64,
            substituted_tasks,
        )


def test_scorer_passes_exact_gates_and_fails_closed() -> None:
    manifest, tasks, results, hashes = _scoring_fixture()
    selection_replay = _selection_replay_fixture(manifest, tasks)
    report = score_handcode_results(
        manifest,
        "e" * 64,
        tasks,
        results,
        hashes,
        confirm_human_only=True,
        selection_replay=selection_replay,
    )
    assert report["gates_passed"] is True
    assert report["routes"]["biography"]["precision_n"] == 60
    assert report["routes"]["issues"]["recall_n"] == 20
    assert report["routes"]["endorsements"]["sampled_presence_rate"] == 1.0
    assert report["known_omission_human_confirmation"]["all_confirmed"] is True
    assert report["known_omission_human_confirmation"]["confirmed_n"] == 11
    assert report["extraction_contract_sha256"] == get_extraction_contract(
        "2.1"
    )["extraction_contract_sha256"]

    with pytest.raises(
        HumanValidationError, match="selection replay is absent or differs"
    ):
        score_handcode_results(
            manifest,
            "e" * 64,
            tasks,
            results,
            hashes,
            confirm_human_only=True,
        )

    wrong_contract = deepcopy(manifest)
    wrong_contract["user_message_template_sha256"] = "0" * 64
    with pytest.raises(HumanValidationError, match="suffix-p contract identity"):
        score_handcode_results(
            wrong_contract,
            "e" * 64,
            tasks,
            results,
            hashes,
            confirm_human_only=True,
            selection_replay=selection_replay,
        )

    wrong_technical_contract = deepcopy(manifest)
    wrong_technical_contract["technical_checks"][
        "endorsement_context_provenance_spec_sha256"
    ] = "0" * 64
    with pytest.raises(HumanValidationError, match="suffix-p contract identity"):
        score_handcode_results(
            wrong_technical_contract,
            "e" * 64,
            tasks,
            results,
            hashes,
            confirm_human_only=True,
            selection_replay=selection_replay,
        )

    incomplete_user_message_replay = deepcopy(manifest)
    incomplete_user_message_replay["technical_checks"][
        "user_message_hash_replay_rate"
    ] = 0.99
    with pytest.raises(HumanValidationError, match="user-message hash replay"):
        score_handcode_results(
            incomplete_user_message_replay,
            "e" * 64,
            tasks,
            results,
            hashes,
            confirm_human_only=True,
            selection_replay=selection_replay,
        )

    mistyped_user_message_replay = deepcopy(manifest)
    mistyped_user_message_replay["technical_checks"][
        "user_message_hash_replay_rate"
    ] = 1
    with pytest.raises(HumanValidationError, match="user-message hash replay"):
        score_handcode_results(
            mistyped_user_message_replay,
            "e" * 64,
            tasks,
            results,
            hashes,
            confirm_human_only=True,
            selection_replay=selection_replay,
        )

    boolean_overlap_count = deepcopy(manifest)
    boolean_overlap_count["technical_checks"][
        "silently_selected_overlap_conflicts"
    ] = False
    with pytest.raises(HumanValidationError, match="overlap conflict"):
        score_handcode_results(
            boolean_overlap_count,
            "e" * 64,
            tasks,
            results,
            hashes,
            confirm_human_only=True,
            selection_replay=selection_replay,
        )

    boolean_truncation_count = deepcopy(manifest)
    boolean_truncation_count["technical_checks"]["truncated_segments"] = False
    with pytest.raises(HumanValidationError, match="truncated segments"):
        score_handcode_results(
            boolean_truncation_count,
            "e" * 64,
            tasks,
            results,
            hashes,
            confirm_human_only=True,
            selection_replay=selection_replay,
        )

    weakened_provenance = deepcopy(manifest)
    weakened_provenance[
        "structured_decoding_disable_any_whitespace"
    ] = False
    with pytest.raises(HumanValidationError, match="whitespace policy"):
        score_handcode_results(
            weakened_provenance,
            "e" * 64,
            tasks,
            results,
            hashes,
            confirm_human_only=True,
            selection_replay=selection_replay,
        )

    weakened_raw_gate = deepcopy(manifest)
    weakened_raw_gate["raw_structural_whitespace_gate_passed"] = False
    with pytest.raises(HumanValidationError, match="raw structural-whitespace"):
        score_handcode_results(
            weakened_raw_gate,
            "e" * 64,
            tasks,
            results,
            hashes,
            confirm_human_only=True,
            selection_replay=selection_replay,
        )

    weakened_technical_raw_gate = deepcopy(manifest)
    weakened_technical_raw_gate["technical_checks"][
        "raw_structural_whitespace_gate_passed"
    ] = False
    with pytest.raises(
        HumanValidationError, match="technical raw structural-whitespace"
    ):
        score_handcode_results(
            weakened_technical_raw_gate,
            "e" * 64,
            tasks,
            results,
            hashes,
            confirm_human_only=True,
            selection_replay=selection_replay,
        )

    weakened_guard_gate = deepcopy(manifest)
    weakened_guard_gate["array_item_cap_reached_outputs"] = 1
    with pytest.raises(HumanValidationError, match="model-item guard gate"):
        score_handcode_results(
            weakened_guard_gate,
            "e" * 64,
            tasks,
            results,
            hashes,
            confirm_human_only=True,
            selection_replay=selection_replay,
        )

    weakened_technical_guard_gate = deepcopy(manifest)
    weakened_technical_guard_gate["technical_checks"][
        "model_item_repetition_error_outputs"
    ] = 1
    with pytest.raises(HumanValidationError, match="model-item guard gate"):
        score_handcode_results(
            weakened_technical_guard_gate,
            "e" * 64,
            tasks,
            results,
            hashes,
            confirm_human_only=True,
            selection_replay=selection_replay,
        )

    boolean_guard_counts = deepcopy(manifest)
    boolean_guard_counts["array_item_cap_reached_outputs"] = False
    boolean_guard_counts["model_item_repetition_error_outputs"] = False
    with pytest.raises(HumanValidationError, match="model-item guard gate"):
        score_handcode_results(
            boolean_guard_counts,
            "e" * 64,
            tasks,
            results,
            hashes,
            confirm_human_only=True,
            selection_replay=selection_replay,
        )

    boolean_technical_guard_counts = deepcopy(manifest)
    boolean_technical_guard_counts["technical_checks"][
        "array_item_cap_reached_outputs"
    ] = False
    boolean_technical_guard_counts["technical_checks"][
        "model_item_repetition_error_outputs"
    ] = False
    with pytest.raises(HumanValidationError, match="model-item guard gate"):
        score_handcode_results(
            boolean_technical_guard_counts,
            "e" * 64,
            tasks,
            results,
            hashes,
            confirm_human_only=True,
            selection_replay=selection_replay,
        )

    boolean_route_cap = deepcopy(manifest)
    boolean_route_cap["route_array_item_caps"]["issues"] = True
    with pytest.raises(HumanValidationError, match="model-item guard gate"):
        score_handcode_results(
            boolean_route_cap,
            "e" * 64,
            tasks,
            results,
            hashes,
            confirm_human_only=True,
            selection_replay=selection_replay,
        )

    unstratified = deepcopy(manifest)
    issue_entries = [
        entry for entry in unstratified["tasks"]
        if entry["route"] == "issues" and entry["kind"] == "precision"
    ]
    first_year = issue_entries[0]["sample_items"][0]["year"]
    for entry in issue_entries:
        for item in entry["sample_items"]:
            item["year"] = first_year
    with pytest.raises(
        HumanValidationError, match="selection replay is absent or differs"
    ):
        score_handcode_results(
            unstratified,
            "e" * 64,
            tasks,
            results,
            hashes,
            confirm_human_only=True,
            selection_replay=selection_replay,
        )

    incomplete = deepcopy(results)
    first_task = next(iter(incomplete))
    incomplete[first_task]["responses"][0]["status"] = "partial"
    with pytest.raises(HumanValidationError, match="incomplete"):
        score_handcode_results(
            manifest,
            "e" * 64,
            tasks,
            incomplete,
            hashes,
            confirm_human_only=True,
            selection_replay=selection_replay,
        )

    failed = deepcopy(results)
    endorsement_task = next(
        entry["task_id"]
        for entry in manifest["tasks"]
        if entry["route"] == "endorsements" and entry["kind"] == "precision"
    )
    failed[endorsement_task]["responses"][0]["answers"][
        "combined_record_context_support"
    ] = "no"
    failed_report = score_handcode_results(
        manifest,
        "e" * 64,
        tasks,
        failed,
        hashes,
        confirm_human_only=True,
        selection_replay=selection_replay,
    )
    assert failed_report["gates_passed"] is False
    assert failed_report["routes"]["endorsements"]["passed"] is False

    omission_failed = deepcopy(results)
    omission_task = next(
        entry["task_id"]
        for entry in manifest["tasks"]
        if entry["kind"] == "known_omission"
    )
    omission_failed[omission_task]["responses"][0]["answers"][
        "semantic_recovery_confirmed"
    ] = "no"
    omission_report = score_handcode_results(
        manifest,
        "e" * 64,
        tasks,
        omission_failed,
        hashes,
        confirm_human_only=True,
        selection_replay=selection_replay,
    )
    assert omission_report["gates_passed"] is False
    assert omission_report["known_omission_human_confirmation"]["confirmed_n"] == 10


def test_finalize_updates_only_human_gate_and_keeps_segments(tmp_path: Path) -> None:
    bundle = tmp_path / "bundle"
    bundle.mkdir()
    segments = bundle / "segments_enriched.jsonl.gz"
    segments.write_bytes(b"frozen-segment-bytes")
    ledger = bundle / "endorsement_line_ledger_private.jsonl.gz"
    ledger.write_bytes(b"frozen-line-ledger-bytes")
    human = bundle / "human_validation.json"
    human.write_text('{"status":"pending"}\n', encoding="utf-8")
    omission_report = bundle / "known_omission_recovery.json"
    omission_report.write_text('{"all_recovered":true}\n', encoding="utf-8")
    segment_spec = {
        "file": segments.name,
        "rows": 1,
        "sha256": sha256_file(segments),
    }
    manifest = {
        "schema_version": "2.1",
        "release_status": "withheld_validation",
        "segments_file": segment_spec,
        "endorsement_line_ledger": {
            "summary": {"public_coverage_complete": True},
            "private_file": {
                "file": ledger.name,
                "rows": 1,
                "sha256": sha256_file(ledger),
            },
        },
        "human_validation_file": {
            "file": human.name,
            "sha256": sha256_file(human),
        },
    }
    manifest_path = bundle / "bundle_manifest.json"
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    sampling = {
        "bundle_manifest_sha256": sha256_file(manifest_path),
        "segments_file": segments.name,
        "segments_file_sha256": sha256_file(segments),
        "known_omission_recovery_file": {
            "file": omission_report.name,
            "sha256": sha256_file(omission_report),
        },
    }
    report = {
        "gates_passed": True,
        "status": "passed",
        "handcode_selection_replay": {
            "version": "candidate_extraction_handcode_selection_replay_v1",
            "passed": True,
            "bundle_manifest_sha256": sampling["bundle_manifest_sha256"],
        },
    }
    before = segments.read_bytes()
    ledger_before = ledger.read_bytes()
    digest = finalize_passing_bundle(bundle, sampling, report)
    final_manifest = json.loads(manifest_path.read_text())
    assert segments.read_bytes() == before
    assert ledger.read_bytes() == ledger_before
    assert final_manifest["segments_file"] == segment_spec
    assert final_manifest["human_validation_file"]["sha256"] == digest
    assert final_manifest["release_status"] == "human_validation_passed"
    assert (bundle / "human_validation.sha256").read_text().startswith(digest)


def test_shared_template_builds_self_contained_html(tmp_path: Path) -> None:
    page = _page(
        "issues",
        1,
        [{"statement_kind": "support", "claim": "A claim", "source_heading": None}],
    )
    record = {**page["records"][0], "page": page}
    payload = precision_task("issues", "candidate_extraction_template_test", [record])
    task_path = tmp_path / "task.json"
    html_path = tmp_path / "task.html"
    task_path.write_text(json.dumps(payload), encoding="utf-8")
    build_task_html(task_path, html_path)
    html = html_path.read_text(encoding="utf-8")
    assert "candidate_extraction_template_test" in html
    assert "localStorage" in html
    assert html_path.stat().st_size > task_path.stat().st_size
