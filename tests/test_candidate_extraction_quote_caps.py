from __future__ import annotations

import argparse
import copy
import gzip
import hashlib
import json
from pathlib import Path

import pytest

from scripts.hive.extract_candidate_pages_vllm import (
    build_user_message,
    chunk_is_complete,
    generation_config_sha256,
    validate_raw_structural_whitespace,
    canonicalize_json_string_controls,
)
from src.candidate_extraction import (
    ALIGNMENT_METHOD_PROVIDED_SEGMENT_EXTENSION,
    ALIGNMENT_METHOD_UNIQUE,
    ALIGNMENT_METHOD_UNIQUE_DISPLAY,
    ARRAY_ITEM_CAP_REACHED_STATUS,
    ARRAY_ITEM_CAP_SPEC_SHA256,
    INFERENCE_DISPLAY_ESCAPE_ID,
    LIST_KEY_BY_VERSION,
    MODEL_ITEM_REPETITION_ERROR_STATUS,
    MODEL_ITEM_REPETITION_SPEC_SHA256,
    MODEL_ID,
    MODEL_REVISION,
    PROMPTS_V2_1,
    PYTHON_STRIP_WHITESPACE_CODEPOINTS,
    RUNTIME_LOCK_SHA256,
    SCHEMAS_V2_1,
    V2_1_MODEL_ITEM_CAPS,
    _v2_1_model_item_schema_valid,
    align_extraction_exact_spans,
    build_endorsement_context_provenance,
    endorsement_context_provenance_errors,
    get_extraction_contract,
    inference_display_escape,
    inference_display_escape_with_boundaries,
    inference_display_provenance,
    inference_display_span_to_source,
    inference_display_unescape,
    model_item_array_cap_report,
    model_item_repetition_report,
    pre_alignment_model_item_guard_report,
    segment_overlap_status,
    validate_extraction,
)


def _walk(node: object):
    yield node
    if isinstance(node, dict):
        for value in node.values():
            yield from _walk(value)
    elif isinstance(node, list):
        for value in node:
            yield from _walk(value)


def _write_gzip_jsonl(path: Path, rows: list[dict[str, object]]) -> None:
    with gzip.open(path, "wt", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")


def _args(tmp_path: Path) -> argparse.Namespace:
    return argparse.Namespace(
        schema_version="2.1",
        model_path=tmp_path / "model",
        seed=20260824,
        max_model_len=32768,
        max_output_tokens=24576,
        temperature=0.0,
        compact_repair=False,
        gpu_memory_utilization=0.92,
        shard_index=None,
        shard_count=None,
    )


def _context(
    parent: str,
    *,
    segment_start: int = 0,
    segment_end: int | None = None,
    segment_id: str = "segment",
    parent_segments: list[dict[str, object]] | None = None,
) -> dict[str, object]:
    if segment_end is None:
        segment_end = len(parent)
    return build_endorsement_context_provenance(
        parent_document_id="parent",
        parent_source_text=parent,
        source_urls=[
            "https://web.archive.org/web/20200101000000/"
            "https://example.test/endorsements"
        ],
        segment_id=segment_id,
        segment_start_char=segment_start,
        segment_end_char=segment_end,
        parent_segments=parent_segments,
    )


def test_quote_display_is_injective_reversible_and_boundary_safe() -> None:
    source = 'A "quoted" e\u0301 🐍 value and literal \\u0022 plus \\t\t\n\x03\x7f\x85'
    display, boundaries = inference_display_escape_with_boundaries(source)
    assert '"' not in display
    assert "\\u0022" in display
    assert "\\\\u0022" in display
    assert "\\\\t" in display
    assert inference_display_unescape(display) == source
    assert inference_display_escape(source) == display

    quote_source_start = source.index('"')
    quote_display_start = display.index("\\u0022")
    assert inference_display_span_to_source(
        source, quote_display_start, quote_display_start + 6
    ) == (quote_source_start, quote_source_start + 1)
    assert all(
        boundaries[index] is None
        for index in range(quote_display_start + 1, quote_display_start + 6)
    )

    literal_source = "\\u0022"
    literal_display, literal_boundaries = inference_display_escape_with_boundaries(
        literal_source
    )
    assert literal_display == "\\\\u0022"
    assert literal_boundaries[1] is None
    with pytest.raises(ValueError, match="inside an escape"):
        inference_display_span_to_source(literal_source, 1, len(literal_display))

    astral_start = source.index("🐍")
    astral_display_start = display.index("🐍")
    assert inference_display_span_to_source(
        source, astral_display_start, astral_display_start + 1
    ) == (astral_start, astral_start + 1)

    provenance = inference_display_provenance(source)
    assert provenance["inference_display_escape_id"] == INFERENCE_DISPLAY_ESCAPE_ID
    assert provenance["inference_display_roundtrip_valid"] is True
    assert provenance["inference_display_escape_codepoint_counts"]["U+0022"] == 2
    assert provenance["inference_display_source_text_sha256"] == hashlib.sha256(
        source.encode("utf-8")
    ).hexdigest()


def test_exact_display_fallback_decodes_quote_with_private_adjustment() -> None:
    source = 'Endorsements\nTexas State Rep. Jolanda "Jo" Jones'
    context = _context(source)
    displayed_record = r"Texas State Rep. Jolanda \u0022Jo\u0022 Jones"
    model = {"endorsements": [{
        "endorser_name": "Jolanda Jones",
        "entity_type": "individual",
        "endorser_category": "government_official",
        "title": "Texas State Rep.",
        "affiliation": None,
        "jurisdiction": None,
        "record_evidence": displayed_record,
        "endorsement_context_evidence": "Endorsements",
        "support_mode": "page_or_section_context",
    }]}
    original = copy.deepcopy(model)
    aligned, withheld, report = align_extraction_exact_spans(
        "endorsements",
        model,
        source,
        endorsement_context_provenance=context,
    )
    assert model == original
    assert withheld == []
    assert report["display_anchor_adjustment_count"] == 1
    adjustment = report["display_anchor_adjustments"][0]
    assert adjustment["model_excerpt"] == displayed_record
    assert adjustment["canonical_display_roundtrip_valid"] is True
    assert len(adjustment["display_anchor_adjustment_id"]) == 64
    item = aligned["endorsements"][0]
    assert item["record_evidence"] == 'Texas State Rep. Jolanda "Jo" Jones'
    assert item["record_evidence_alignment_method"] == (
        ALIGNMENT_METHOD_UNIQUE_DISPLAY
    )
    assert validate_extraction(
        "endorsements",
        aligned,
        source,
        "2.1",
        endorsement_context_provenance=context,
        parent_source_text=source,
    )["schema_valid"] is True

    # Exact source matching always has precedence. A literal source backslash
    # spelling must never be mistaken for a displayed source quote.
    literal_source = r"Policy literal \u0022 marker and one \"quoted\" word"
    issue_model = {"issue_blocks": [{
        "source_heading": None,
        "claim": "Mentions the literal marker",
        "statement_kind": "unclear",
        "evidence": r"\u0022",
    }]}
    literal_aligned, literal_withheld, literal_report = (
        align_extraction_exact_spans("issues", issue_model, literal_source)
    )
    assert literal_withheld == []
    literal_item = literal_aligned["issue_blocks"][0]
    assert literal_item["evidence"] == r"\u0022"
    assert literal_item["evidence_alignment_method"] == ALIGNMENT_METHOD_UNIQUE
    assert literal_report["display_anchor_adjustment_count"] == 0

    ambiguous_source = 'Alice "Ace" Doe and Alice "Ace" Doe'
    ambiguous_model = {"issue_blocks": [{
        "source_heading": None,
        "claim": "Mentions Alice Ace Doe",
        "statement_kind": "unclear",
        "evidence": r'Alice \u0022Ace\u0022 Doe',
    }]}
    ambiguous_aligned, ambiguous_withheld, ambiguous_report = (
        align_extraction_exact_spans(
            "issues", ambiguous_model, ambiguous_source
        )
    )
    assert ambiguous_aligned == {"issue_blocks": []}
    assert ambiguous_withheld[0]["reason"] == "ambiguous_exact_span"
    assert ambiguous_report["display_anchor_adjustment_count"] == 0

    # The same fail-closed inverse applies to explicit endorsement context.
    # This is a separate copied anchor even when it equals record evidence.
    explicit_source = 'Alice "Ace" Doe endorses the candidate'
    explicit_context = _context(explicit_source)
    explicit_display = r'Alice \u0022Ace\u0022 Doe endorses the candidate'
    explicit_model = {"endorsements": [{
        "endorser_name": 'Alice "Ace" Doe',
        "entity_type": "individual",
        "endorser_category": "private_individual",
        "title": None,
        "affiliation": None,
        "jurisdiction": None,
        "record_evidence": explicit_display,
        "endorsement_context_evidence": explicit_display,
        "support_mode": "explicit_text",
    }]}
    explicit_aligned, explicit_withheld, explicit_report = (
        align_extraction_exact_spans(
            "endorsements",
            explicit_model,
            explicit_source,
            endorsement_context_provenance=explicit_context,
        )
    )
    assert explicit_withheld == []
    assert explicit_report["display_anchor_adjustment_count"] == 2
    explicit_item = explicit_aligned["endorsements"][0]
    assert explicit_item["record_evidence"] == explicit_source
    assert explicit_item["endorsement_context_evidence"] == explicit_source
    assert explicit_item["endorsement_context_evidence_alignment_method"] == (
        ALIGNMENT_METHOD_UNIQUE_DISPLAY
    )
    assert validate_extraction(
        "endorsements",
        explicit_aligned,
        explicit_source,
        "2.1",
        endorsement_context_provenance=explicit_context,
        parent_source_text=explicit_source,
    )["schema_valid"] is True


def test_exact_segment_context_extension_projects_to_carried_span() -> None:
    source = (
        "Endorsements:\n"
        "Individuals who endorse Wendell Champion for Congress:\n"
        "Paula Abbott Barry"
    )
    context = _context(source)
    model_context = (
        "Endorsements:\n"
        "Individuals who endorse Wendell Champion for Congress:"
    )
    model = {"endorsements": [{
        "endorser_name": "Paula Abbott Barry",
        "entity_type": "individual",
        "endorser_category": "private_individual",
        "title": None,
        "affiliation": None,
        "jurisdiction": None,
        "record_evidence": "Paula Abbott Barry",
        "endorsement_context_evidence": model_context,
        "support_mode": "page_or_section_context",
    }]}
    aligned, withheld, report = align_extraction_exact_spans(
        "endorsements",
        model,
        source,
        endorsement_context_provenance=context,
    )
    assert withheld == []
    assert report["endorsement_context_extension_count"] == 1
    extension = report["endorsement_context_extensions"][0]
    assert extension["model_context_excerpt"] == model_context
    assert extension["record_disposition"] == "aligned"
    item = aligned["endorsements"][0]
    assert item["endorsement_context_evidence"] == "Endorsements:"
    assert item["endorsement_context_evidence_alignment_method"] == (
        ALIGNMENT_METHOD_PROVIDED_SEGMENT_EXTENSION
    )
    assert source[
        item["endorsement_context_evidence_start_char"]:
        item["endorsement_context_evidence_end_char"]
    ] == "Endorsements:"
    assert validate_extraction(
        "endorsements",
        aligned,
        source,
        "2.1",
        endorsement_context_provenance=context,
        parent_source_text=source,
    )["schema_valid"] is True

    unsafe = copy.deepcopy(model)
    unsafe["endorsements"][0]["endorsement_context_evidence"] = (
        "Individuals who endorse Wendell Champion for Congress:"
    )
    unsafe_aligned, unsafe_withheld, unsafe_report = align_extraction_exact_spans(
        "endorsements",
        unsafe,
        source,
        endorsement_context_provenance=context,
    )
    assert unsafe_aligned == {"endorsements": []}
    assert unsafe_withheld[0]["reason"] == "endorsement_context_mismatch"
    assert unsafe_report["endorsement_context_extension_count"] == 0

    record_including = copy.deepcopy(model)
    record_including["endorsements"][0]["endorsement_context_evidence"] = source
    _aligned, record_including_withheld, record_including_report = (
        align_extraction_exact_spans(
            "endorsements",
            record_including,
            source,
            endorsement_context_provenance=context,
        )
    )
    assert record_including_withheld[0]["reason"] == (
        "endorsement_context_mismatch"
    )
    assert record_including_report["endorsement_context_extension_count"] == 0

    duplicated_source = source + "\n" + model_context
    duplicated_context = _context(duplicated_source)
    _aligned, duplicated_withheld, duplicated_report = (
        align_extraction_exact_spans(
            "endorsements",
            model,
            duplicated_source,
            endorsement_context_provenance=duplicated_context,
        )
    )
    assert duplicated_withheld[0]["reason"] == "endorsement_context_mismatch"
    assert duplicated_report["endorsement_context_extension_count"] == 0

    quoted_source = (
        "Endorsements:\n"
        'People who endorse "Ace":\n'
        "Alice Smith"
    )
    quoted_context = _context(quoted_source)
    quoted_model = {"endorsements": [{
        "endorser_name": "Alice Smith",
        "entity_type": "individual",
        "endorser_category": "private_individual",
        "title": None,
        "affiliation": None,
        "jurisdiction": None,
        "record_evidence": "Alice Smith",
        "endorsement_context_evidence": (
            r'Endorsements:\nPeople who endorse \u0022Ace\u0022:'
        ).replace(r"\n", "\n"),
        "support_mode": "page_or_section_context",
    }]}
    quoted_aligned, quoted_withheld, quoted_report = (
        align_extraction_exact_spans(
            "endorsements",
            quoted_model,
            quoted_source,
            endorsement_context_provenance=quoted_context,
        )
    )
    assert quoted_withheld == []
    assert len(quoted_aligned["endorsements"]) == 1
    quoted_extension = quoted_report["endorsement_context_extensions"][0]
    assert quoted_extension["model_context_source_alignment_method"] == (
        ALIGNMENT_METHOD_UNIQUE_DISPLAY
    )
    assert quoted_extension[
        "model_context_display_exact_boundary_match_count"
    ] == 1


def test_endorsement_partial_line_boundaries_are_attested_and_withheld() -> None:
    parent = (
        'Endorsements\nTra"cy Ruffeno\nJeff "Ace" Smith\n'
        "Tail Partial Name\nFooter"
    )
    segment_start = parent.index('Tra"cy Ruffeno') + len("Tra")
    segment_end = parent.index("Tail Partial Name") + len("Tail")
    segment = parent[segment_start:segment_end]
    context = _context(
        parent,
        segment_start=segment_start,
        segment_end=segment_end,
        segment_id="continuation",
        parent_segments=[{
            "document_id": "heading-segment",
            "segment_start_char": 0,
            "segment_end_char": segment_start,
        }],
    )
    assert context["segment_starts_mid_line"] is True
    assert context["segment_ends_mid_line"] is True
    assert context["segment_has_complete_line"] is True
    first_complete = segment.index("\n") + 1
    last_complete = segment.rindex("\n") + 1
    assert context["segment_first_complete_line_start_char"] == first_complete
    assert context["segment_last_complete_line_end_char"] == last_complete
    assert segment[first_complete:last_complete] == 'Jeff "Ace" Smith\n'
    first_display = len(inference_display_escape(segment[:first_complete]))
    last_display = len(inference_display_escape(segment[:last_complete]))
    assert first_display > first_complete
    assert last_display > last_complete
    assert context[
        "segment_first_complete_line_inference_display_start_char"
    ] == first_display
    assert context[
        "segment_last_complete_line_inference_display_end_char"
    ] == last_display

    def item(name: str, evidence: str) -> dict[str, object]:
        return {
            "endorser_name": name,
            "entity_type": "individual",
            "endorser_category": "private_individual",
            "title": None,
            "affiliation": None,
            "jurisdiction": None,
            "record_evidence": evidence,
            "endorsement_context_evidence": "Endorsements",
            "support_mode": "page_or_section_context",
        }

    model = {"endorsements": [
        item("cy Ruffeno", "cy Ruffeno"),
        item('Jeff "Ace" Smith', r'Jeff \u0022Ace\u0022 Smith'),
        item("Tail", "Tail"),
    ]}
    aligned, withheld, report = align_extraction_exact_spans(
        "endorsements",
        model,
        segment,
        endorsement_context_provenance=context,
        segment_start_char=segment_start,
    )
    assert [row["endorser_name"] for row in aligned["endorsements"]] == [
        'Jeff "Ace" Smith'
    ]
    assert [row["reason"] for row in withheld] == [
        "clipped_segment_boundary",
        "clipped_segment_boundary",
    ]
    assert report["withheld_item_counts_by_reason"] == {
        "clipped_segment_boundary": 2
    }
    assert validate_extraction(
        "endorsements",
        aligned,
        segment,
        "2.1",
        endorsement_context_provenance=context,
        segment_start_char=segment_start,
        parent_source_text=parent,
    )["schema_valid"] is True

    assert segment_overlap_status(0, 1) == "single_segment"
    assert segment_overlap_status(0, 3) == "first_segment_with_trailing_overlap"
    assert segment_overlap_status(1, 3) == (
        "middle_segment_with_leading_and_trailing_overlap"
    )
    assert segment_overlap_status(2, 3) == "last_segment_with_leading_overlap"

    user_record = {
        "candidate_icpsr": "12345",
        "office": "House",
        "state": "TX",
        "year": 2024,
        "party": "D",
        "route": "endorsements",
        "source_urls": ["https://example.test/endorsements"],
        "segment_index": 2,
        "segment_count": 3,
        "endorsement_context_provenance": context,
    }
    message = build_user_message(user_record, segment, "2.1")
    assert "Page text starts mid-line: true" in message
    assert "Segment has a complete line: true" in message
    assert f"source-text line starts at local character: {first_complete}" in message
    assert f"inference-display character: {first_display}" in message
    assert "Page text ends mid-line: true" in message
    assert f"source-text line ends at local character: {last_complete}" in message
    assert f"inference-display character: {last_display}" in message
    assert "Source-text complete-line offsets are provenance only" in message
    assert "Segment index (zero-based): 2" in message
    assert "Segment count: 3" in message
    assert "last_segment_with_leading_overlap" in message

    # The clipped-line rule is endorsement-only. Issue and biography evidence
    # can be complete even when an overlapping source window begins mid-line.
    issue_source = 'partial source says support the "K Street Project"'
    issue_model = {"issue_blocks": [{
        "source_heading": None,
        "claim": "Supports the K Street Project",
        "statement_kind": "support",
        "evidence": issue_source,
    }]}
    issue_aligned, issue_withheld, _issue_report = align_extraction_exact_spans(
        "issues", issue_model, issue_source
    )
    assert issue_withheld == []
    assert len(issue_aligned["issue_blocks"]) == 1


def test_endorsement_segment_without_complete_line_withholds_every_item() -> None:
    parent = "prefixAlice Smithsuffix"
    segment_start = len("prefix")
    segment_end = len(parent) - len("suffix")
    segment = parent[segment_start:segment_end]
    context = _context(
        parent,
        segment_start=segment_start,
        segment_end=segment_end,
        segment_id="inside-one-line",
    )
    assert segment == "Alice Smith"
    assert context["segment_starts_mid_line"] is True
    assert context["segment_ends_mid_line"] is True
    assert context["segment_has_complete_line"] is False
    for field in (
        "segment_first_complete_line_start_char",
        "segment_first_complete_line_parent_start_char",
        "segment_first_complete_line_inference_display_start_char",
        "segment_last_complete_line_end_char",
        "segment_last_complete_line_parent_end_char",
        "segment_last_complete_line_inference_display_end_char",
    ):
        assert context[field] is None
    assert endorsement_context_provenance_errors(
        context,
        parent_source_text=parent,
        segment_source_text=segment,
        segment_id="inside-one-line",
        segment_start_char=segment_start,
        parent_document_id="parent",
        source_urls=[
            "https://web.archive.org/web/20200101000000/"
            "https://example.test/endorsements"
        ],
    ) == []
    model = {"endorsements": [{
        "endorser_name": "Alice Smith",
        "entity_type": "individual",
        "endorser_category": "private_individual",
        "title": None,
        "affiliation": None,
        "jurisdiction": None,
        "record_evidence": "Alice Smith",
        "endorsement_context_evidence": None,
        "support_mode": "page_or_section_context",
    }]}
    aligned, withheld, report = align_extraction_exact_spans(
        "endorsements",
        model,
        segment,
        endorsement_context_provenance=context,
        segment_start_char=segment_start,
    )
    assert aligned == {"endorsements": []}
    assert withheld[0]["reason"] == "clipped_segment_boundary"
    assert report["withheld_item_counts_by_reason"] == {
        "clipped_segment_boundary": 1
    }


def test_overlapping_local_heading_binds_current_segment_not_prior_carrier() -> None:
    parent = "Intro\nEndorsements\nAlice Smith\n"
    heading_start = parent.index("Endorsements")
    prior = {
        "document_id": "prior-overlap",
        "segment_start_char": 0,
        "segment_end_char": parent.index("Alice Smith") + 5,
    }
    current = {
        "document_id": "current-segment",
        "segment_start_char": heading_start,
        "segment_end_char": len(parent),
    }
    context = _context(
        parent,
        segment_start=heading_start,
        segment_end=len(parent),
        segment_id="current-segment",
        parent_segments=[prior, current],
    )
    assert context["source_scope"] == "segment"
    assert context["context_source_segment_id"] == "current-segment"
    assert context["context_source_segment_start_char"] == heading_start
    assert context["context_local_start_char"] == 0
    assert context["segment_starts_mid_line"] is False
    assert context["segment_ends_mid_line"] is False
    assert context["segment_has_complete_line"] is True
    assert context["segment_first_complete_line_start_char"] == 0
    assert context["segment_last_complete_line_end_char"] == len(
        parent[heading_start:]
    )
    assert endorsement_context_provenance_errors(
        context,
        segment_source_text=parent[heading_start:],
        segment_id="current-segment",
        segment_start_char=heading_start,
        parent_document_id="parent",
        source_urls=[
            "https://web.archive.org/web/20200101000000/"
            "https://example.test/endorsements"
        ],
    ) == []
    wrong_segment = copy.deepcopy(context)
    wrong_segment["context_source_segment_id"] = "prior-overlap"
    context_payload = {
        key: value for key, value in wrong_segment.items() if key != "context_id"
    }
    wrong_segment["context_id"] = hashlib.sha256(json.dumps(
        context_payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")).hexdigest()
    assert "endorsement_text_context_segment_id_mismatch" in (
        endorsement_context_provenance_errors(
            wrong_segment,
            segment_source_text=parent[heading_start:],
            segment_id="current-segment",
            segment_start_char=heading_start,
            parent_document_id="parent",
            source_urls=[
                "https://web.archive.org/web/20200101000000/"
                "https://example.test/endorsements"
            ],
        )
    )


def test_quote_prompt_distinguishes_quote_escape_from_literal_backslash() -> None:
    prompt = PROMPTS_V2_1["issues"]
    assert "visible six-character \\u0022" in prompt
    assert "Copy displayed \\u0022 as a JSON\nUnicode escape" in prompt
    assert "displayed\n\\\\u0022 represents a literal source backslash" in prompt
    assert "Never repeat an identical\nrecord or cycle" in prompt
    endorsement_prompt = PROMPTS_V2_1["endorsements"]
    assert "continuation segments that begin in the middle of a list" in (
        endorsement_prompt
    )
    assert "Skip a clipped or incomplete first line" in endorsement_prompt
    assert "candidate's own name" in endorsement_prompt
    assert "visible inference-display character range" in endorsement_prompt
    assert "Segment has a complete line is false, return no endorsements" in (
        endorsement_prompt
    )


def test_v2_1_guided_schemas_have_caps_and_nullable_ablation() -> None:
    issue_properties = SCHEMAS_V2_1["issues"]["properties"]["issue_blocks"][
        "items"
    ]["properties"]
    biography_branches = SCHEMAS_V2_1["biography"]["properties"]["facts"][
        "items"
    ]["oneOf"]
    endorsement_branches = SCHEMAS_V2_1["endorsements"]["properties"][
        "endorsements"
    ]["items"]["oneOf"]
    exact_anchor_nodes = [
        issue_properties["evidence"],
        *(branch["properties"]["evidence"] for branch in biography_branches),
        *(
            branch["properties"]["record_evidence"]
            for branch in endorsement_branches
        ),
        endorsement_branches[0]["properties"][
            "endorsement_context_evidence"
        ],
    ]
    exact_anchor_ids = {id(node) for node in exact_anchor_nodes}
    for route, schema in SCHEMAS_V2_1.items():
        list_key = LIST_KEY_BY_VERSION["2.1"][route]
        route_array_schema = schema["properties"][list_key]
        if route == "endorsements":
            assert "maxItems" not in route_array_schema
        else:
            assert route_array_schema["maxItems"] == V2_1_MODEL_ITEM_CAPS[route]
        for node in _walk(schema):
            if not isinstance(node, dict):
                continue
            assert "pattern" not in node
            if node.get("type") == ["string", "null"]:
                assert "minLength" not in node
            if node.get("type") == "string" and "enum" not in node:
                if id(node) in exact_anchor_ids:
                    assert node == {"type": "string"}
                else:
                    assert node.get("minLength") == 1
    assert len(exact_anchor_nodes) == 7

    # xgrammar guidance deliberately accepts empty nullable strings; the
    # independent Python model-item validator fails them closed.
    nullable_heading = SCHEMAS_V2_1["issues"]["properties"]["issue_blocks"][
        "items"
    ]["properties"]["source_heading"]
    assert nullable_heading == {"type": ["string", "null"]}
    issue = {
        "source_heading": "",
        "claim": "Supports the policy",
        "statement_kind": "support",
        "evidence": "Supports the policy",
    }
    assert not _v2_1_model_item_schema_valid("issues", issue)
    endorsement = {
        "endorser_name": "Jane Doe",
        "entity_type": "individual",
        "endorser_category": "private_individual",
        "title": "",
        "affiliation": None,
        "jurisdiction": None,
        "record_evidence": "Jane Doe supports Alex",
        "endorsement_context_evidence": "Jane Doe supports Alex",
        "support_mode": "explicit_text",
    }
    assert not _v2_1_model_item_schema_valid("endorsements", endorsement)

    for codepoint in PYTHON_STRIP_WHITESPACE_CODEPOINTS:
        whitespace = chr(codepoint)
        issue["source_heading"] = whitespace
        assert not _v2_1_model_item_schema_valid("issues", issue)
    issue["source_heading"] = "["
    assert _v2_1_model_item_schema_valid("issues", issue)


def test_array_cap_report_fails_at_or_above_cap() -> None:
    key = LIST_KEY_BY_VERSION["2.1"]["issues"]
    cap = V2_1_MODEL_ITEM_CAPS["issues"]
    below = model_item_array_cap_report("issues", {key: [{}] * (cap - 1)})
    at = model_item_array_cap_report("issues", {key: [{}] * cap})
    above = model_item_array_cap_report("issues", {key: [{}] * (cap + 1)})
    assert below["array_item_cap_validation_passed"] is True
    assert below["failure_status"] is None
    for report in (at, above):
        assert report["array_item_cap_reached"] is True
        assert report["array_item_cap_validation_passed"] is False
        assert report["failure_status"] == ARRAY_ITEM_CAP_REACHED_STATUS
        assert report["array_item_cap_spec_sha256"] == ARRAY_ITEM_CAP_SPEC_SHA256


def test_exact_item_repetition_catches_triples_and_alternating_cycles() -> None:
    key = LIST_KEY_BY_VERSION["2.1"]["issues"]
    a = {"claim": "A", "evidence": "A", "source_heading": None, "statement_kind": "support"}
    b = {"claim": "B", "evidence": "B", "source_heading": None, "statement_kind": "oppose"}
    double = model_item_repetition_report("issues", {key: [a, b, a, b]})
    assert double["max_exact_model_item_multiplicity"] == 2
    assert double["model_item_repetition_validation_passed"] is True

    triple = model_item_repetition_report("issues", {key: [a, a, a]})
    assert triple["model_item_repetition_validation_passed"] is False
    assert triple["failure_status"] == MODEL_ITEM_REPETITION_ERROR_STATUS
    assert triple["model_item_repetition_spec_sha256"] == MODEL_ITEM_REPETITION_SPEC_SHA256

    key_order_equivalent = {
        "statement_kind": "support",
        "source_heading": None,
        "evidence": "A",
        "claim": "A",
    }
    reordered = model_item_repetition_report(
        "issues", {key: [a, key_order_equivalent, copy.deepcopy(a)]}
    )
    assert reordered["max_exact_model_item_multiplicity"] == 3
    assert reordered["model_item_repetition_validation_passed"] is False

    alternating = pre_alignment_model_item_guard_report(
        "issues", {key: [a, b, a, b, a, b]}
    )
    assert alternating["failure_status"] == MODEL_ITEM_REPETITION_ERROR_STATUS
    assert alternating["model_item_repetition_report"][
        "repeated_model_item_group_count"
    ] == 2

    cap_items = [a, b] * (V2_1_MODEL_ITEM_CAPS["issues"] // 2)
    both = pre_alignment_model_item_guard_report("issues", {key: cap_items})
    assert both["failure_status"] == ARRAY_ITEM_CAP_REACHED_STATUS


def test_restart_identity_requires_quote_and_model_item_guard_attestations(
    tmp_path: Path,
) -> None:
    source_text = 'The candidate says "support this policy."'
    model_extraction = {
        "issue_blocks": [{
            "source_heading": None,
            "claim": "Supports this policy",
            "statement_kind": "support",
            "evidence": '"support this policy."',
        }]
    }
    aligned, withheld, alignment = align_extraction_exact_spans(
        "issues", model_extraction, source_text
    )
    assert aligned is not None and not withheld
    validation = validate_extraction(
        "issues", aligned, source_text, schema_version="2.1"
    )
    assert validation["schema_valid"] is True
    contract = get_extraction_contract("2.1")
    args = _args(tmp_path)
    generation_hash = generation_config_sha256(args, contract)
    raw_output = json.dumps(model_extraction, ensure_ascii=False, sort_keys=True)
    canonical, canonicalization = canonicalize_json_string_controls(raw_output)
    assert canonical == raw_output
    structural = validate_raw_structural_whitespace(raw_output)
    assert structural["raw_structural_whitespace_validation_passed"] is True
    display = inference_display_provenance(source_text)
    guard = pre_alignment_model_item_guard_report("issues", model_extraction)

    input_path = tmp_path / "chunk_00000.jsonl.gz"
    output_path = tmp_path / "chunk_00000.out.jsonl.gz"
    input_row = {
        "document_id": "doc",
        "candidate_cycle_id": "C-doc",
        "candidate_icpsr": "12345",
        "office": "House",
        "state": "TX",
        "year": 2024,
        "party": "D",
        "source_urls": [
            "https://example.test/issues/primary",
            "https://example.test/issues/secondary",
        ],
        "route": "issues",
        "raw_text_sha256": "a" * 64,
        "parent_document_id": "parent-doc",
        "segment_index": 0,
        "segment_count": 1,
        "segment_start_char": 0,
        "segment_end_char": len(source_text),
        "segment_text_sha256": hashlib.sha256(
            source_text.encode("utf-8")
        ).hexdigest(),
        "source_text": source_text,
    }
    user_message_sha256 = hashlib.sha256(
        build_user_message(input_row, source_text, "2.1").encode("utf-8")
    ).hexdigest()
    output_row = {
        "document_id": "doc",
        "candidate_cycle_id": "C-doc",
        "route": "issues",
        "raw_text_sha256": "a" * 64,
        "parent_document_id": "parent-doc",
        "segment_index": 0,
        "segment_count": 1,
        "segment_start_char": 0,
        "segment_end_char": len(source_text),
        "segment_text_sha256": hashlib.sha256(
            source_text.encode("utf-8")
        ).hexdigest(),
        "model_id": MODEL_ID,
        "model_revision": MODEL_REVISION,
        "runtime_lock_sha256": RUNTIME_LOCK_SHA256,
        "schema_version": "2.1",
        "prompt_sha256": contract["prompt_sha256"]["issues"],
        "json_schema_sha256": contract["json_schema_sha256"]["issues"],
        "extraction_contract_sha256": contract["extraction_contract_sha256"],
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
        "user_message_sha256": user_message_sha256,
        "structured_decoding_backend": "xgrammar",
        "structured_decoding_disable_any_whitespace": True,
        "json_string_control_canonicalizer_id": contract[
            "json_string_control_canonicalizer_id"
        ],
        "json_string_control_canonicalizer_spec_sha256": contract[
            "json_string_control_canonicalizer_spec_sha256"
        ],
        "array_item_cap_spec_id": contract["array_item_cap_spec_id"],
        "array_item_cap_spec_sha256": contract["array_item_cap_spec_sha256"],
        "route_array_item_cap": contract["route_array_item_caps"]["issues"],
        "model_item_repetition_spec_id": contract[
            "model_item_repetition_spec_id"
        ],
        "model_item_repetition_spec_sha256": contract[
            "model_item_repetition_spec_sha256"
        ],
        "raw_output": raw_output,
        "model_extraction": model_extraction,
        "model_extraction_sha256": hashlib.sha256(
            json.dumps(
                model_extraction,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
        ).hexdigest(),
        "extraction": aligned,
        "aligned_extraction_sha256": alignment["aligned_extraction_sha256"],
        "withheld_model_items": withheld,
        "pre_alignment_model_item_guard": guard,
        "array_item_cap_report": guard["array_item_cap_report"],
        "model_item_repetition_report": guard["model_item_repetition_report"],
        "exact_alignment": alignment,
        "validation": validation,
        "parse_status": "parsed",
        "generation_config_sha256": generation_hash,
        "finish_reason": "stop",
        "stop_reason": None,
        "output_tokens": 20,
        "max_output_tokens": 24576,
        "temperature": 0.0,
        "thinking_enabled": False,
        "compact_repair": False,
        "input_truncated": False,
        "input_tokens_used": 10,
        "input_chars_used": len(source_text),
        **display,
        **canonicalization,
        **structural,
    }
    _write_gzip_jsonl(input_path, [input_row])
    _write_gzip_jsonl(output_path, [output_row])
    assert chunk_is_complete(
        input_path,
        output_path,
        schema_version="2.1",
        expected_generation_config_sha256=generation_hash,
        expected_max_output_tokens=24_576,
    )

    for input_mutation in (
        {"candidate_icpsr": "54321"},
        {"source_urls": list(reversed(input_row["source_urls"]))},
    ):
        tampered_input = copy.deepcopy(input_row)
        tampered_input.update(input_mutation)
        _write_gzip_jsonl(input_path, [tampered_input])
        assert not chunk_is_complete(
            input_path,
            output_path,
            schema_version="2.1",
            expected_generation_config_sha256=generation_hash,
            expected_max_output_tokens=24_576,
        )
    _write_gzip_jsonl(input_path, [input_row])

    for field, tampered_value in (
        ("segment_index", 0.0),
        ("segment_index", False),
        ("input_chars_used", float(len(source_text))),
        ("input_tokens_used", False),
    ):
        type_mismatch = copy.deepcopy(output_row)
        type_mismatch[field] = tampered_value
        _write_gzip_jsonl(output_path, [type_mismatch])
        assert not chunk_is_complete(
            input_path,
            output_path,
            schema_version="2.1",
            expected_generation_config_sha256=generation_hash,
            expected_max_output_tokens=24_576,
        )

    nested_type_mismatch = copy.deepcopy(output_row)
    nested_type_mismatch["array_item_cap_report"]["model_item_count"] = 1.0
    _write_gzip_jsonl(output_path, [nested_type_mismatch])
    assert not chunk_is_complete(
        input_path,
        output_path,
        schema_version="2.1",
        expected_generation_config_sha256=generation_hash,
        expected_max_output_tokens=24_576,
    )

    # Standalone public validation can verify final source exactness but cannot
    # reconstruct which raw-model adjustment produced an exact final span.
    # The restart gate binds that private provenance by recomputing alignment.
    method_mismatch = copy.deepcopy(output_row)
    method_mismatch["extraction"]["issue_blocks"][0][
        "evidence_alignment_method"
    ] = ALIGNMENT_METHOD_UNIQUE_DISPLAY
    _write_gzip_jsonl(output_path, [method_mismatch])
    assert not chunk_is_complete(
        input_path,
        output_path,
        schema_version="2.1",
        expected_generation_config_sha256=generation_hash,
        expected_max_output_tokens=24_576,
    )

    extra_field = copy.deepcopy(output_row)
    extra_field["unexpected_restart_field"] = "not allowed"
    _write_gzip_jsonl(output_path, [extra_field])
    assert not chunk_is_complete(
        input_path,
        output_path,
        schema_version="2.1",
        expected_generation_config_sha256=generation_hash,
        expected_max_output_tokens=24_576,
    )

    for status in (
        MODEL_ITEM_REPETITION_ERROR_STATUS,
        ARRAY_ITEM_CAP_REACHED_STATUS,
    ):
        failed = copy.deepcopy(output_row)
        failed["parse_status"] = status
        _write_gzip_jsonl(output_path, [failed])
        assert not chunk_is_complete(
            input_path,
            output_path,
            schema_version="2.1",
            expected_generation_config_sha256=generation_hash,
            expected_max_output_tokens=24_576,
        )

    raw_mismatch = copy.deepcopy(output_row)
    raw_mismatch["raw_output"] = '{"issue_blocks": []}'
    canonical_mismatch, canonicalization_mismatch = (
        canonicalize_json_string_controls(raw_mismatch["raw_output"])
    )
    assert canonical_mismatch == raw_mismatch["raw_output"]
    raw_mismatch.update(canonicalization_mismatch)
    raw_mismatch.update(validate_raw_structural_whitespace(
        raw_mismatch["raw_output"]
    ))
    _write_gzip_jsonl(output_path, [raw_mismatch])
    assert not chunk_is_complete(
        input_path,
        output_path,
        schema_version="2.1",
        expected_generation_config_sha256=generation_hash,
        expected_max_output_tokens=24_576,
    )

    route_mismatch = copy.deepcopy(output_row)
    route_mismatch["route"] = "biography"
    route_mismatch["prompt_sha256"] = contract["prompt_sha256"]["biography"]
    route_mismatch["json_schema_sha256"] = contract["json_schema_sha256"][
        "biography"
    ]
    route_mismatch["route_array_item_cap"] = contract["route_array_item_caps"][
        "biography"
    ]
    _write_gzip_jsonl(output_path, [route_mismatch])
    assert not chunk_is_complete(
        input_path,
        output_path,
        schema_version="2.1",
        expected_generation_config_sha256=generation_hash,
        expected_max_output_tokens=24_576,
    )

    aligned_mismatch = copy.deepcopy(output_row)
    aligned_mismatch["extraction"]["issue_blocks"][0]["claim"] = "Tampered"
    _write_gzip_jsonl(output_path, [aligned_mismatch])
    assert not chunk_is_complete(
        input_path,
        output_path,
        schema_version="2.1",
        expected_generation_config_sha256=generation_hash,
        expected_max_output_tokens=24_576,
    )

    char_count_mismatch = copy.deepcopy(output_row)
    char_count_mismatch["input_chars_used"] = len(source_text) + 1
    _write_gzip_jsonl(output_path, [char_count_mismatch])
    assert not chunk_is_complete(
        input_path,
        output_path,
        schema_version="2.1",
        expected_generation_config_sha256=generation_hash,
        expected_max_output_tokens=24_576,
    )

    token_cap_mismatch = copy.deepcopy(output_row)
    token_cap_mismatch["max_output_tokens"] = 30_000
    _write_gzip_jsonl(output_path, [token_cap_mismatch])
    assert not chunk_is_complete(
        input_path,
        output_path,
        schema_version="2.1",
        expected_generation_config_sha256=generation_hash,
        expected_max_output_tokens=24_576,
    )
