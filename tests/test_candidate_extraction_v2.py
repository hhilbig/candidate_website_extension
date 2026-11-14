"""Focused contract tests for candidate extraction schema v2."""
from __future__ import annotations

import copy
import gzip
import hashlib
import json
import subprocess
import sys
from pathlib import Path

import pytest

from scripts.export_candidate_extraction import segment_document
from scripts.hive.extract_candidate_pages_vllm import (
    GENERATION_CONFIG_VERSION,
    RAW_STRUCTURAL_WHITESPACE_VALIDATOR_ID,
    RAW_STRUCTURAL_WHITESPACE_VALIDATOR_SPEC_SHA256,
    STRUCTURED_DECODING_BACKEND,
    STRUCTURED_DECODING_DISABLE_ANY_WHITESPACE,
    build_user_message,
    canonicalize_json_string_controls,
    chunk_is_complete,
    generation_config_payload,
    generation_config_sha256,
    llm_engine_kwargs,
    run_metadata_name,
    run_metadata_payload,
    strict_json_loads,
    trim_to_budget,
    validate_raw_structural_whitespace,
    V2_1_MIN_OUTPUT_TOKENS,
)
from src.candidate_extraction import (
    ALIGNMENT_METHOD_AMBIGUOUS,
    ALIGNMENT_METHOD_MISSING,
    ALIGNMENT_METHOD_NOT_APPLICABLE,
    ALIGNMENT_METHOD_PAGE_ROUTE,
    ALIGNMENT_METHOD_PROVIDED_PARENT,
    ALIGNMENT_METHOD_PROVIDED_SEGMENT,
    ALIGNMENT_METHOD_UNIQUE,
    ENDORSEMENT_CONTEXT_CLASSIFIER_SPEC_SHA256,
    ENDORSEMENT_CONTEXT_PROVENANCE_SPEC_SHA256,
    EXACT_ALIGNMENT_SPEC_SHA256,
    FROZEN_V2_CONTRACT_SHA256,
    FROZEN_V2_1_ALIGNMENT_SPEC_SHA256,
    FROZEN_V2_1_CONTRACT_SHA256,
    FROZEN_V2_1_ENDORSEMENT_CONTEXT_PROVENANCE_SPEC_SHA256,
    FROZEN_V2_1_INFERENCE_DISPLAY_ESCAPE_SPEC_SHA256,
    FROZEN_V2_1_JSON_STRING_CONTROL_CANONICALIZER_SPEC_SHA256,
    FROZEN_V2_1_PROMPT_SHA256,
    FROZEN_V2_1_SCHEMA_SHA256,
    FROZEN_V2_1_SEMANTIC_VALIDATION_SPEC_SHA256,
    FROZEN_V2_1_USER_MESSAGE_TEMPLATE_SHA256,
    FROZEN_V2_PROMPT_SHA256,
    FROZEN_V2_SCHEMA_SHA256,
    MODEL_ID,
    MODEL_REVISION,
    RUNTIME_LOCK_SHA256,
    INFERENCE_DISPLAY_ESCAPE_ID,
    INFERENCE_DISPLAY_ESCAPE_SPEC_SHA256,
    JSON_STRING_CONTROL_CANONICALIZER_ID,
    JSON_STRING_CONTROL_CANONICALIZER_SPEC_SHA256,
    PYTHON_STRIP_WHITESPACE_CODEPOINTS,
    PROMPTS_V2_1,
    SEMANTIC_VALIDATION_SPEC_SHA256,
    SCHEMAS_V2,
    SCHEMAS_V2_1,
    USER_MESSAGE_TEMPLATE_V2_1_SHA256,
    _v2_1_model_item_schema_valid,
    align_extraction_exact_spans,
    build_endorsement_context_provenance,
    computed_contract_hashes,
    exact_occurrence_spans,
    exact_span_is_valid,
    get_extraction_contract,
    inference_display_escape,
    inference_display_escape_with_boundaries,
    inference_display_provenance,
    inference_display_unescape,
    pre_alignment_model_item_guard_report,
    parent_char_span,
    resolve_overlap_variants,
    validate_extraction,
)


def _span(source: str, excerpt: str) -> tuple[int, int]:
    start = source.index(excerpt)
    return start, start + len(excerpt)


def _write_jsonl_gz(path: Path, rows: list[dict]) -> None:
    with gzip.open(path, "wt", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")


def _focused_json_schema_accepts(schema: dict, value: object) -> bool:
    """Evaluate the small JSON Schema subset used by v2.1 model items."""
    if "oneOf" in schema:
        if sum(
            _focused_json_schema_accepts(branch, value)
            for branch in schema["oneOf"]
        ) != 1:
            return False

    expected_types = schema.get("type")
    if expected_types is not None:
        if isinstance(expected_types, str):
            expected_types = [expected_types]
        type_matches = {
            "array": lambda item: isinstance(item, list),
            "integer": lambda item: (
                isinstance(item, int) and not isinstance(item, bool)
            ),
            "null": lambda item: item is None,
            "object": lambda item: isinstance(item, dict),
            "string": lambda item: isinstance(item, str),
        }
        if not any(type_matches[kind](value) for kind in expected_types):
            return False

    if "enum" in schema and value not in schema["enum"]:
        return False
    if isinstance(value, str):
        if len(value) < schema.get("minLength", 0):
            return False
    if isinstance(value, dict) and "properties" in schema:
        properties = schema["properties"]
        if not set(schema.get("required", ())).issubset(value):
            return False
        if schema.get("additionalProperties") is False and not set(value).issubset(
            properties
        ):
            return False
        if any(
            not _focused_json_schema_accepts(properties[key], item)
            for key, item in value.items()
            if key in properties
        ):
            return False
    if isinstance(value, list) and "items" in schema:
        if any(
            not _focused_json_schema_accepts(schema["items"], item)
            for item in value
        ):
            return False
    return True


def _v2_1_item_schema(route: str) -> dict:
    list_key = {
        "issues": "issue_blocks",
        "biography": "facts",
        "endorsements": "endorsements",
    }[route]
    return SCHEMAS_V2_1[route]["properties"][list_key]["items"]


def _v2_1_exact_anchor_nodes() -> list[dict]:
    issue = _v2_1_item_schema("issues")["properties"]
    biography = _v2_1_item_schema("biography")["oneOf"]
    endorsements = _v2_1_item_schema("endorsements")["oneOf"]
    return [
        issue["evidence"],
        *(branch["properties"]["evidence"] for branch in biography),
        *(branch["properties"]["record_evidence"] for branch in endorsements),
        endorsements[0]["properties"]["endorsement_context_evidence"],
    ]


def _v2_1_generative_nonblank_nodes() -> list[dict]:
    issue = _v2_1_item_schema("issues")["properties"]
    biography = _v2_1_item_schema("biography")["oneOf"]
    endorsements = _v2_1_item_schema("endorsements")["oneOf"]
    return [
        issue["claim"],
        *(branch["properties"]["value"] for branch in biography),
        biography[0]["properties"]["relationship"],
        *(branch["properties"]["endorser_name"] for branch in endorsements),
    ]


def _schema_nodes(value: object):
    if isinstance(value, dict):
        yield value
        for child in value.values():
            yield from _schema_nodes(child)
    elif isinstance(value, list):
        for child in value:
            yield from _schema_nodes(child)


def _endorsement_context(
    parent_source: str,
    *,
    segment_start: int = 0,
    segment_end: int | None = None,
    segment_id: str = "segment-1",
) -> dict:
    return build_endorsement_context_provenance(
        parent_document_id="parent-1",
        parent_source_text=parent_source,
        source_urls=[
            "https://web.archive.org/web/20200101000000/"
            "https://example.test/endorsements/"
        ],
        segment_id=segment_id,
        segment_start_char=segment_start,
        segment_end_char=(len(parent_source) if segment_end is None else segment_end),
    )


def test_v2_prompt_schema_and_combined_contract_hashes_are_frozen() -> None:
    computed = computed_contract_hashes("2.0")
    assert computed["prompt_sha256"] == FROZEN_V2_PROMPT_SHA256
    assert computed["json_schema_sha256"] == FROZEN_V2_SCHEMA_SHA256
    assert computed["extraction_contract_sha256"] == FROZEN_V2_CONTRACT_SHA256
    assert get_extraction_contract("v2") == computed


def test_v2_1_prompt_schema_alignment_and_semantic_hashes_are_frozen() -> None:
    computed = computed_contract_hashes("2.1")
    assert computed["prompt_sha256"] == FROZEN_V2_1_PROMPT_SHA256
    assert computed["json_schema_sha256"] == FROZEN_V2_1_SCHEMA_SHA256
    assert computed["alignment_spec_sha256"] == FROZEN_V2_1_ALIGNMENT_SPEC_SHA256
    assert computed["alignment_spec_sha256"] == EXACT_ALIGNMENT_SPEC_SHA256
    assert (
        computed["semantic_validation_spec_sha256"]
        == FROZEN_V2_1_SEMANTIC_VALIDATION_SPEC_SHA256
        == SEMANTIC_VALIDATION_SPEC_SHA256
    )
    assert (
        computed["user_message_template_sha256"]
        == FROZEN_V2_1_USER_MESSAGE_TEMPLATE_SHA256
        == USER_MESSAGE_TEMPLATE_V2_1_SHA256
    )
    assert (
        computed["endorsement_context_provenance_spec_sha256"]
        == FROZEN_V2_1_ENDORSEMENT_CONTEXT_PROVENANCE_SPEC_SHA256
        == ENDORSEMENT_CONTEXT_PROVENANCE_SPEC_SHA256
    )
    assert (
        computed["inference_display_escape_spec_sha256"]
        == FROZEN_V2_1_INFERENCE_DISPLAY_ESCAPE_SPEC_SHA256
        == INFERENCE_DISPLAY_ESCAPE_SPEC_SHA256
    )
    assert (
        computed["json_string_control_canonicalizer_spec_sha256"]
        == FROZEN_V2_1_JSON_STRING_CONTROL_CANONICALIZER_SPEC_SHA256
        == JSON_STRING_CONTROL_CANONICALIZER_SPEC_SHA256
    )
    assert computed["extraction_contract_sha256"] == FROZEN_V2_1_CONTRACT_SHA256
    assert get_extraction_contract("v2.1") == computed
    for route in ("issues", "biography", "endorsements"):
        schema_text = json.dumps(SCHEMAS_V2_1[route], sort_keys=True)
        assert "start_char" not in schema_text
        assert "end_char" not in schema_text


def test_page_type_classifier_import_does_not_require_beautifulsoup() -> None:
    code = """
import builtins
original_import = builtins.__import__
def blocked_import(name, *args, **kwargs):
    if name == 'bs4' or name.startswith('bs4.'):
        raise ModuleNotFoundError('blocked for classifier-only runtime test')
    return original_import(name, *args, **kwargs)
builtins.__import__ = blocked_import
from src.extract_text import classify_page_type
assert classify_page_type(
    'https://web.archive.org/web/20200101000000/https://example.test/supporters/'
) == 'endorsements'
"""
    subprocess.run([sys.executable, "-c", code], check=True)


def test_v2_1_prompts_and_schemas_require_general_exhaustive_clause_extraction() -> None:
    issue_prompt = PROMPTS_V2_1["issues"].lower()
    biography_prompt = PROMPTS_V2_1["biography"].lower()
    assert "beginning to end" in issue_prompt
    assert "introductory" in issue_prompt
    assert "coordinated sentences and lists" in issue_prompt
    assert "gerund or participial" in issue_prompt
    assert "approval or disapproval" in issue_prompt
    assert "exact heading is not unique" in issue_prompt
    assert "paragraph by paragraph" in biography_prompt
    assert "one-off initiatives" in biography_prompt
    assert "distinct projects or actions" in biography_prompt
    endorsement_prompt = PROMPTS_V2_1["endorsements"].lower()
    assert "carried context rule above overrides" in endorsement_prompt
    assert "parent_document scope" in endorsement_prompt
    assert "return null for page_or_section_route_context" in endorsement_prompt
    assert "record_evidence must still be copied exactly" in endorsement_prompt

    issue_properties = SCHEMAS_V2_1["issues"]["properties"]["issue_blocks"][
        "items"
    ]["properties"]
    assert set(issue_properties) == {
        "source_heading", "claim", "statement_kind", "evidence",
    }
    biography_branches = _v2_1_item_schema("biography")["oneOf"]
    assert all(set(branch["properties"]) == {
        "field", "value", "timing", "relationship", "evidence",
    } for branch in biography_branches)
    endorsement_branches = _v2_1_item_schema("endorsements")["oneOf"]
    assert all(set(branch["properties"]) == {
        "endorser_name", "entity_type", "endorser_category", "title",
        "affiliation", "jurisdiction", "record_evidence",
        "endorsement_context_evidence", "support_mode",
    } for branch in endorsement_branches)
    schema_descriptions = json.dumps({
        "issues": SCHEMAS_V2_1["issues"],
        "biography": SCHEMAS_V2_1["biography"],
    }).lower()
    assert "source order" in schema_descriptions
    assert "coordinated or listed" in schema_descriptions
    assert "public-service project or action" in schema_descriptions

    # The revision states general extraction rules, not tuning-set backfills.
    frozen_instruction_text = issue_prompt + biography_prompt + schema_descriptions
    for tuning_phrase in (
        "afghanistan", "k-mart", "medicare", "pueblos", "hepatitis b",
        "supreme court",
    ):
        assert tuning_phrase not in frozen_instruction_text


def test_v2_1_guided_item_schemas_use_decoder_safe_disjoint_branches() -> None:
    forbidden_keywords = {"$ref", "allOf", "anyOf", "const", "if", "then", "else"}
    exact_anchor_node_ids = {id(node) for node in _v2_1_exact_anchor_nodes()}
    for route in ("issues", "biography", "endorsements"):
        for node in _schema_nodes(SCHEMAS_V2_1[route]):
            assert not forbidden_keywords.intersection(node)
            assert "pattern" not in node
            node_types = node.get("type", ())
            if isinstance(node_types, str):
                node_types = (node_types,)
            if "string" in node_types and "enum" not in node:
                if "null" in node_types:
                    assert "minLength" not in node
                elif id(node) in exact_anchor_node_ids:
                    assert node == {"type": "string"}
                else:
                    assert node.get("minLength") == 1
    assert len(_v2_1_exact_anchor_nodes()) == 7
    assert all(
        node == {"type": "string"} for node in _v2_1_exact_anchor_nodes()
    )
    assert all(
        node == {"type": "string", "minLength": 1}
        for node in _v2_1_generative_nonblank_nodes()
    )

    biography_branches = _v2_1_item_schema("biography")["oneOf"]
    assert len(biography_branches) == 3
    biography_field_groups = [
        set(branch["properties"]["field"]["enum"])
        for branch in biography_branches
    ]
    assert biography_field_groups == [
        {"local_roots"},
        {
            "occupation", "elected_office_experience",
            "public_service_experience",
        },
        {"education", "military_service"},
    ]
    assert len(set.union(*biography_field_groups)) == sum(
        len(group) for group in biography_field_groups
    )

    endorsement_branches = _v2_1_item_schema("endorsements")["oneOf"]
    assert len(endorsement_branches) == 2
    assert [
        branch["properties"]["support_mode"]["enum"]
        for branch in endorsement_branches
    ] == [["explicit_text"], ["page_or_section_context"]]

    for branch in [*biography_branches, *endorsement_branches]:
        assert branch["type"] == "object"
        assert branch["additionalProperties"] is False
        assert set(branch["required"]) == set(branch["properties"])


def test_v2_1_min_length_guidance_leaves_unicode_whitespace_to_python_gate() -> None:
    actual_python_strip_whitespace = tuple(
        codepoint
        for codepoint in range(sys.maxunicode + 1)
        if chr(codepoint).strip() == ""
    )
    assert actual_python_strip_whitespace == PYTHON_STRIP_WHITESPACE_CODEPOINTS

    for route in ("issues", "biography", "endorsements"):
        for node in _schema_nodes(SCHEMAS_V2_1[route]):
            assert "pattern" not in node

    # Generative nonblank fields retain minLength. Exact-copy anchors omit it
    # because xgrammar 0.2.3 suppresses quote escapes on such fields; the
    # independent Python validator rejects empty and Unicode-whitespace values.
    assert not _focused_json_schema_accepts({"type": "string", "minLength": 1}, "")
    for node in _v2_1_exact_anchor_nodes():
        assert _focused_json_schema_accepts(node, "")
        assert _focused_json_schema_accepts(node, "\u3000")
        assert _focused_json_schema_accepts(node, 'literal "quote"')
    for codepoint in PYTHON_STRIP_WHITESPACE_CODEPOINTS:
        value = chr(codepoint)
        assert _focused_json_schema_accepts(
            {"type": "string", "minLength": 1}, value
        )
        assert value.strip() == ""
    for value in ("[", "Å", "候補者", "🙂", "\t[\u3000"):
        assert _focused_json_schema_accepts(
            {"type": "string", "minLength": 1}, value
        )
        assert value.strip()

    representative_items = {
        "issues": ({
            "source_heading": None,
            "claim": "Supports transit",
            "statement_kind": "support",
            "evidence": "I support transit.",
        }, "claim"),
        "biography": ({
            "field": "education",
            "value": "State University",
            "timing": None,
            "relationship": None,
            "evidence": "attended State University",
        }, "value"),
        "endorsements": ({
            "endorser_name": "Alex Smith",
            "entity_type": "individual",
            "endorser_category": "private_individual",
            "title": None,
            "affiliation": None,
            "jurisdiction": None,
            "record_evidence": "Alex Smith",
            "endorsement_context_evidence": None,
            "support_mode": "page_or_section_context",
        }, "endorser_name"),
    }
    for codepoint in PYTHON_STRIP_WHITESPACE_CODEPOINTS:
        for route, (item, field) in representative_items.items():
            whitespace_item = copy.deepcopy(item)
            whitespace_item[field] = chr(codepoint)
            assert _focused_json_schema_accepts(
                _v2_1_item_schema(route), whitespace_item
            )
            assert not _v2_1_model_item_schema_valid(route, whitespace_item)


def test_v2_1_guided_schemas_match_python_model_item_validation() -> None:
    whitespace = "".join(chr(value) for value in PYTHON_STRIP_WHITESPACE_CODEPOINTS)

    def check(route: str, item: dict, expected: bool) -> None:
        guided = _focused_json_schema_accepts(_v2_1_item_schema(route), item)
        python_gate = _v2_1_model_item_schema_valid(route, item)
        assert guided is expected
        assert python_gate is expected

    def check_python_backstop(route: str, item: dict) -> None:
        assert _focused_json_schema_accepts(_v2_1_item_schema(route), item)
        assert not _v2_1_model_item_schema_valid(route, item)

    def changed(item: dict, **updates: object) -> dict:
        result = copy.deepcopy(item)
        result.update(updates)
        return result

    issue = {
        "source_heading": None,
        "claim": "Supports candidate aid [now]",
        "statement_kind": "support",
        "evidence": "I support candidate aid [now].",
    }
    check("issues", issue, True)
    check("issues", changed(issue, source_heading="政策 [A]"), True)
    check_python_backstop("issues", changed(issue, source_heading=""))
    check_python_backstop("issues", changed(issue, source_heading=whitespace))
    check("issues", changed(issue, claim=""), False)
    check_python_backstop("issues", changed(issue, claim=whitespace))
    check_python_backstop("issues", changed(issue, evidence=""))
    check_python_backstop("issues", changed(issue, evidence=whitespace))
    check("issues", changed(issue, evidence='I support "candidate aid".'), True)
    check("issues", changed(issue, statement_kind="preference"), False)
    missing_issue_field = copy.deepcopy(issue)
    missing_issue_field.pop("claim")
    check("issues", missing_issue_field, False)
    check("issues", changed(issue, normalized_topic="health"), False)

    biography_items = {
        "local_roots": {
            "field": "local_roots", "value": "Born in Davis",
            "timing": "former", "relationship": "Born in",
            "evidence": "Born in Davis",
        },
        "occupation": {
            "field": "occupation", "value": "Teacher", "timing": "current",
            "relationship": None, "evidence": "works as a teacher",
        },
        "elected_office_experience": {
            "field": "elected_office_experience", "value": "Mayor",
            "timing": "former", "relationship": None,
            "evidence": "formerly served as mayor",
        },
        "public_service_experience": {
            "field": "public_service_experience", "value": "Food drive",
            "timing": "unclear", "relationship": None,
            "evidence": "organized a food drive",
        },
        "education": {
            "field": "education", "value": "State University", "timing": None,
            "relationship": None, "evidence": "attended State University",
        },
        "military_service": {
            "field": "military_service", "value": "Army veteran", "timing": None,
            "relationship": None, "evidence": "served in the Army",
        },
    }
    for item in biography_items.values():
        check("biography", item, True)
        check("biography", changed(item, value="[Candidate]"), True)
        check("biography", changed(item, value=""), False)
        check_python_backstop(
            "biography", changed(item, value=whitespace)
        )
        check_python_backstop("biography", changed(item, evidence=""))
        check_python_backstop(
            "biography", changed(item, evidence=whitespace)
        )
        check(
            "biography", changed(item, evidence='received the "Award"'), True
        )
    check_python_backstop(
        "biography", changed(biography_items["education"], value=whitespace),
    )
    for field in (
        "local_roots", "occupation", "elected_office_experience",
        "public_service_experience",
    ):
        check("biography", changed(biography_items[field], timing=None), False)
    for field in ("education", "military_service"):
        check(
            "biography", changed(biography_items[field], timing="unclear"), True
        )
    check(
        "biography", changed(biography_items["local_roots"], relationship=None),
        False,
    )
    check(
        "biography",
        changed(biography_items["local_roots"], relationship=""),
        False,
    )
    check_python_backstop(
        "biography",
        changed(biography_items["local_roots"], relationship=whitespace),
    )
    for field in (
        "occupation", "elected_office_experience", "public_service_experience",
        "education", "military_service",
    ):
        check(
            "biography",
            changed(biography_items[field], relationship="worked in"),
            False,
        )
    check("biography", changed(biography_items["education"], timing="past"), False)
    check("biography", changed(biography_items["education"], field="family"), False)
    missing_biography_field = copy.deepcopy(biography_items["education"])
    missing_biography_field.pop("evidence")
    check("biography", missing_biography_field, False)
    check(
        "biography", changed(biography_items["education"], other="private"), False
    )

    explicit_endorsement = {
        "endorser_name": "Alex [A]",
        "entity_type": "individual",
        "endorser_category": "government_official",
        "title": None,
        "affiliation": None,
        "jurisdiction": None,
        "record_evidence": "Alex [A] supports the candidate.",
        "endorsement_context_evidence": "supports the candidate",
        "support_mode": "explicit_text",
    }
    route_endorsement = changed(
        explicit_endorsement,
        endorsement_context_evidence=None,
        support_mode="page_or_section_context",
    )
    check("endorsements", explicit_endorsement, True)
    check("endorsements", route_endorsement, True)
    check(
        "endorsements",
        changed(route_endorsement, endorsement_context_evidence="[Endorsements]"),
        True,
    )
    check_python_backstop(
        "endorsements", changed(route_endorsement, title=whitespace)
    )
    check(
        "endorsements",
        changed(explicit_endorsement, endorsement_context_evidence=None),
        False,
    )
    for item in (explicit_endorsement, route_endorsement):
        check("endorsements", changed(item, endorser_name=""), False)
        check_python_backstop(
            "endorsements", changed(item, endorser_name=whitespace)
        )
        check_python_backstop(
            "endorsements", changed(item, record_evidence="")
        )
        check_python_backstop(
            "endorsements", changed(item, record_evidence=whitespace)
        )
        for field in ("title", "affiliation", "jurisdiction"):
            check("endorsements", changed(item, **{field: None}), True)
            check("endorsements", changed(item, **{field: "[value]"}), True)
            check_python_backstop(
                "endorsements", changed(item, **{field: ""})
            )
            check_python_backstop(
                "endorsements", changed(item, **{field: whitespace})
            )
    check_python_backstop(
        "endorsements",
        changed(explicit_endorsement, endorsement_context_evidence=""),
    )
    check_python_backstop(
        "endorsements",
        changed(explicit_endorsement, endorsement_context_evidence=whitespace),
    )
    check_python_backstop(
        "endorsements",
        changed(route_endorsement, endorsement_context_evidence=whitespace),
    )
    check("endorsements", changed(route_endorsement, entity_type="person"), False)
    check(
        "endorsements", changed(route_endorsement, endorser_category="union"),
        False,
    )
    check("endorsements", changed(route_endorsement, support_mode="implicit"), False)
    missing_endorsement_field = copy.deepcopy(route_endorsement)
    missing_endorsement_field.pop("title")
    check("endorsements", missing_endorsement_field, False)
    check("endorsements", changed(route_endorsement, organization="legacy"), False)

    quote_endorsement = changed(
        explicit_endorsement,
        record_evidence='Jane "Q" Doe endorses Alex.',
        endorsement_context_evidence='"endorses" Alex',
    )
    check("endorsements", quote_endorsement, True)


@pytest.mark.parametrize("bad_anchor", ["", "\u3000"])
def test_v2_1_empty_or_whitespace_exact_anchors_never_align_publicly(
    bad_anchor: str,
) -> None:
    cases = [
        (
            "issues",
            "issue_blocks",
            {"issue_blocks": [{
                "source_heading": None,
                "claim": "Supports transit",
                "statement_kind": "support",
                "evidence": bad_anchor,
            }]},
            "I support transit.",
            None,
        ),
        (
            "biography",
            "facts",
            {"facts": [{
                "field": "occupation",
                "value": "Teacher",
                "timing": "current",
                "relationship": None,
                "evidence": bad_anchor,
            }]},
            "works as a teacher",
            None,
        ),
        (
            "endorsements",
            "endorsements",
            {"endorsements": [{
                "endorser_name": "Jane Doe",
                "entity_type": "individual",
                "endorser_category": "private_individual",
                "title": None,
                "affiliation": None,
                "jurisdiction": None,
                "record_evidence": bad_anchor,
                "endorsement_context_evidence": "Jane Doe endorses Alex.",
                "support_mode": "explicit_text",
            }]},
            "Endorsements\nJane Doe endorses Alex.",
            _endorsement_context("Endorsements\nJane Doe endorses Alex."),
        ),
        (
            "endorsements",
            "endorsements",
            {"endorsements": [{
                "endorser_name": "Jane Doe",
                "entity_type": "individual",
                "endorser_category": "private_individual",
                "title": None,
                "affiliation": None,
                "jurisdiction": None,
                "record_evidence": "Jane Doe endorses Alex.",
                "endorsement_context_evidence": bad_anchor,
                "support_mode": "explicit_text",
            }]},
            "Endorsements\nJane Doe endorses Alex.",
            _endorsement_context("Endorsements\nJane Doe endorses Alex."),
        ),
    ]
    for route, list_key, model, source, context in cases:
        aligned, withheld, report = align_extraction_exact_spans(
            route,
            model,
            source,
            endorsement_context_provenance=context,
        )
        assert aligned == {list_key: []}
        assert [item["reason"] for item in withheld] == ["invalid_item_schema"]
        assert report["status"] == "invalid_schema"
        assert report["model_schema_valid"] is False
        assert report["all_model_items_accounted_for"] is True


def test_v2_exact_span_uses_half_open_unicode_code_point_offsets() -> None:
    source = "A🙂 candidate’s exact\nwords"
    excerpt = "candidate’s exact\nwords"
    start, end = _span(source, excerpt)
    assert exact_span_is_valid(source, excerpt, start, end)
    assert source[start:end] == excerpt
    assert not exact_span_is_valid(source, "candidate's exact words", start, end)
    assert not exact_span_is_valid(source, excerpt, start + 1, end)
    assert not exact_span_is_valid(source, excerpt, True, end)
    assert parent_char_span(100, start, end) == (100 + start, 100 + end)


def test_v2_1_alignment_is_exact_unique_and_preserves_original_model_object() -> None:
    source = "Jobs🙂\nWe support collective bargaining."
    model = {
        "issue_blocks": [{
            "source_heading": "Jobs🙂",
            "claim": "Supports collective bargaining",
            "statement_kind": "support",
            "evidence": "We support collective bargaining.",
        }]
    }
    original = json.loads(json.dumps(model, ensure_ascii=False))
    aligned, withheld, report = align_extraction_exact_spans("issues", model, source)
    assert model == original
    assert withheld == []
    assert report["status"] == "complete"
    assert report["model_item_count"] == report["aligned_item_count"] == 1
    assert report["withheld_item_count"] == 0
    assert report["all_model_items_accounted_for"] is True
    assert report["original_model_extraction_sha256"] == report["model_extraction_sha256"]
    assert report["aligned_extraction_sha256"]
    item = aligned["issue_blocks"][0]
    assert item["evidence_alignment_method"] == ALIGNMENT_METHOD_UNIQUE
    assert item["evidence_exact_match_count"] == 1
    assert source[item["evidence_start_char"]:item["evidence_end_char"]] == item["evidence"]
    result = validate_extraction("issues", aligned, source, "2.1")
    assert result["schema_valid"]
    assert result["n_supported"] == result["n_items"] == 1
    assert result["n_exact_anchors"] == result["n_required_anchors"] == 2
    again, again_withheld, again_report = align_extraction_exact_spans(
        "issues", model, source
    )
    assert again == aligned and again_withheld == withheld
    assert again_report == report


def test_v2_1_alignment_accepts_verified_fixed_span_without_ambiguous_research() -> None:
    source = "Jobs\nWe support unions.\nWe support unions."
    model = {
        "issue_blocks": [{
            "source_heading": "Jobs",
            "claim": "Supports unions",
            "statement_kind": "support",
            "evidence": "We support unions.",
        }]
    }
    start = source.rindex("We support unions.")
    end = start + len("We support unions.")
    aligned, withheld, report = align_extraction_exact_spans(
        "issues", model, source,
        fixed_anchor_spans={0: {"evidence": (start, end)}},
    )
    assert withheld == []
    assert report["status"] == "complete"
    item = aligned["issue_blocks"][0]
    assert (item["evidence_start_char"], item["evidence_end_char"]) == (start, end)
    assert item["evidence_alignment_method"] == (
        "provided_authenticated_parent_projection_v1"
    )


def test_v2_1_alignment_withholds_ambiguous_missing_and_semantic_failures() -> None:
    source = "Endorsements\nJane Doe\nJane Doe\n123 Main Street"
    model = {
        "facts": [
            {
                "field": "local_roots",
                "value": "123 Main Street",
                "timing": "unclear",
                "relationship": "lives at",
                "evidence": "123 Main Street",
            },
            {
                "field": "education",
                "value": "State University",
                "timing": None,
                "relationship": None,
                "evidence": "not in the supplied source",
            },
        ]
    }
    aligned, withheld, report = align_extraction_exact_spans("biography", model, source)
    assert aligned == {"facts": []}
    assert [item["reason"] for item in withheld] == [
        "invalid_item_semantics", "missing_exact_span",
    ]
    assert withheld[0]["semantic_errors"] == [
        "local_roots_relationship_not_verbatim_in_evidence",
        "local_roots_address_like_text_prohibited",
    ]
    assert report["withheld_item_counts_by_reason"] == {
        "invalid_item_semantics": 1,
        "missing_exact_span": 1,
    }
    assert report["all_model_items_accounted_for"] is True
    assert len(set(report["withheld_source_model_item_ids"])) == 2

    issue_model = {
        "issue_blocks": [{
            "source_heading": None,
            "claim": "Mentions Jane Doe",
            "statement_kind": "unclear",
            "evidence": "Jane Doe",
        }]
    }
    issue_aligned, issue_withheld, issue_report = align_extraction_exact_spans(
        "issues", issue_model, source
    )
    assert issue_aligned == {"issue_blocks": []}
    assert issue_withheld[0]["reason"] == "ambiguous_exact_span"
    assert issue_withheld[0]["anchors"]["evidence"]["exact_match_count"] == 2
    assert issue_report["status"] == "all_withheld"
    assert exact_occurrence_spans(source, "Jane Doe") == [(13, 21), (22, 30)]


def test_v2_1_nullable_heading_has_explicit_not_applicable_provenance() -> None:
    source = "I support unions."
    model = {"issue_blocks": [{
        "source_heading": None,
        "claim": "Supports unions",
        "statement_kind": "support",
        "evidence": source,
    }]}
    aligned, withheld, _report = align_extraction_exact_spans("issues", model, source)
    assert not withheld
    item = aligned["issue_blocks"][0]
    assert item["source_heading_start_char"] is None
    assert item["source_heading_end_char"] is None
    assert item["source_heading_exact_match_count"] == 0
    assert item["source_heading_alignment_method"] == ALIGNMENT_METHOD_NOT_APPLICABLE
    item["evidence_exact_match_count"] = 2
    assert not validate_extraction("issues", aligned, source, "2.1")["schema_valid"]


@pytest.mark.parametrize(
    ("source", "heading", "model_method", "model_match_count"),
    [
        (
            "Policy\nWe oppose blanket mandates.\nPolicy",
            "Policy",
            ALIGNMENT_METHOD_AMBIGUOUS,
            2,
        ),
        (
            "We oppose blanket mandates.",
            "Unstated heading",
            ALIGNMENT_METHOD_MISSING,
            0,
        ),
    ],
)
def test_v2_1_nonunique_optional_heading_is_nulled_without_losing_record(
    source: str,
    heading: str,
    model_method: str,
    model_match_count: int,
) -> None:
    model = {"issue_blocks": [{
        "source_heading": heading,
        "claim": "Opposes blanket mandates",
        "statement_kind": "oppose",
        "evidence": "We oppose blanket mandates.",
    }]}
    original = json.loads(json.dumps(model))
    aligned, withheld, report = align_extraction_exact_spans(
        "issues", model, source
    )
    assert model == original
    assert withheld == []
    assert report["status"] == "complete"
    assert report["optional_anchor_adjustment_count"] == 1
    adjustment = report["optional_anchor_adjustments"][0]
    assert adjustment["model_excerpt"] == heading
    assert adjustment["model_exact_match_count"] == model_match_count
    assert adjustment["model_alignment_method"] == model_method
    assert adjustment["alignment_disposition"] == "optional_anchor_nulled"
    assert adjustment["record_disposition"] == "aligned"
    assert len(adjustment["optional_anchor_adjustment_id"]) == 64

    item = aligned["issue_blocks"][0]
    assert item["source_heading"] is None
    assert item["source_heading_start_char"] is None
    assert item["source_heading_end_char"] is None
    assert item["source_heading_exact_match_count"] == 0
    assert item["source_heading_alignment_method"] == ALIGNMENT_METHOD_NOT_APPLICABLE
    result = validate_extraction("issues", aligned, source, "2.1")
    assert result["schema_valid"]
    assert result["n_supported"] == result["n_items"] == 1
    assert result["n_exact_anchors"] == result["n_required_anchors"] == 1


def test_v2_1_endorsement_context_gate_keeps_supported_list_entry_only() -> None:
    source = "Endorsements\nJane Doe, Mayor of Davis"
    base = {
        "endorser_name": "Jane Doe",
        "entity_type": "individual",
        "endorser_category": "government_official",
        "title": "Mayor",
        "affiliation": None,
        "jurisdiction": "Davis",
        "record_evidence": "Jane Doe, Mayor of Davis",
        "endorsement_context_evidence": "Endorsements",
        "support_mode": "page_or_section_context",
    }
    provenance = _endorsement_context(source)
    aligned, withheld, report = align_extraction_exact_spans(
        "endorsements",
        {"endorsements": [base]},
        source,
        endorsement_context_provenance=provenance,
    )
    assert withheld == [] and report["status"] == "complete"
    assert aligned["endorsements"][0][
        "endorsement_context_evidence_alignment_method"
    ] == ALIGNMENT_METHOD_PROVIDED_SEGMENT
    assert validate_extraction(
        "endorsements", aligned, source, "2.1",
        endorsement_context_provenance=provenance,
    )["schema_valid"]

    direct_context_source = "Jane Doe endorses Alice."
    direct_context = {
        **base,
        "record_evidence": direct_context_source,
        "endorsement_context_evidence": direct_context_source,
        "support_mode": "explicit_text",
    }
    direct_provenance = _endorsement_context(direct_context_source)
    aligned_direct, withheld_direct, _ = align_extraction_exact_spans(
        "endorsements",
        {"endorsements": [direct_context]},
        direct_context_source,
        endorsement_context_provenance=direct_provenance,
    )
    assert withheld_direct == []
    assert validate_extraction(
        "endorsements", aligned_direct, direct_context_source, "2.1",
        endorsement_context_provenance=direct_provenance,
    )["schema_valid"]

    bare_explicit = {
        **base,
        "endorsement_context_evidence": base["record_evidence"],
        "support_mode": "explicit_text",
    }
    _aligned, withheld, report = align_extraction_exact_spans(
        "endorsements",
        {"endorsements": [bare_explicit]},
        source,
        endorsement_context_provenance=provenance,
    )
    assert report["status"] == "all_withheld"
    assert withheld[0]["reason"] == "invalid_item_semantics"
    assert "explicit_text_record_lacks_direct_support_marker" in withheld[0][
        "semantic_errors"
    ]


def test_v2_1_parent_context_uses_explicit_duplicate_occurrence_and_offsets() -> None:
    parent = "Endorsements\nNavigation\nEndorsements\nOrganizations\nJane Doe"
    segment_start = parent.index("Organizations")
    segment = parent[segment_start:]
    provenance = _endorsement_context(
        parent,
        segment_start=segment_start,
        segment_id="segment-parent-context",
    )
    assert provenance["source_scope"] == "parent_document"
    assert provenance["context_parent_exact_match_count"] == 2
    assert provenance["context_parent_occurrence_ordinal"] == 2
    assert parent[
        provenance["context_parent_start_char"]:
        provenance["context_parent_end_char"]
    ] == "Endorsements"
    model = {"endorsements": [{
        "endorser_name": "Jane Doe",
        "entity_type": "individual",
        "endorser_category": "private_individual",
        "title": None,
        "affiliation": None,
        "jurisdiction": None,
        "record_evidence": "Jane Doe",
        "endorsement_context_evidence": "Endorsements",
        "support_mode": "page_or_section_context",
    }]}
    aligned, withheld, report = align_extraction_exact_spans(
        "endorsements",
        model,
        segment,
        endorsement_context_provenance=provenance,
        segment_start_char=segment_start,
    )
    assert withheld == [] and report["status"] == "complete"
    item = aligned["endorsements"][0]
    assert item["endorsement_context_evidence_source_scope"] == "parent_document"
    assert item["endorsement_context_evidence_start_char"] is None
    assert item["endorsement_context_evidence_end_char"] is None
    assert item["endorsement_context_evidence_alignment_method"] == (
        ALIGNMENT_METHOD_PROVIDED_PARENT
    )
    assert item["endorsement_context_evidence_exact_match_count"] == 2
    assert item["endorsement_context_evidence_parent_start_char"] == (
        provenance["context_parent_start_char"]
    )
    validation = validate_extraction(
        "endorsements",
        aligned,
        segment,
        "2.1",
        endorsement_context_provenance=provenance,
        segment_start_char=segment_start,
        parent_source_text=parent,
    )
    assert validation["schema_valid"] and validation["n_supported"] == 1


def test_v2_1_url_route_context_is_explicit_nullable_provenance() -> None:
    source = "Organizations\nCommunity Leaders\nJane Doe"
    provenance = _endorsement_context(source)
    assert provenance["source_scope"] == "page_or_section_route_context"
    assert provenance["context_text"] is None
    assert provenance["url_classifier_spec_sha256"] == (
        ENDORSEMENT_CONTEXT_CLASSIFIER_SPEC_SHA256
    )
    model = {"endorsements": [{
        "endorser_name": "Jane Doe",
        "entity_type": "individual",
        "endorser_category": "private_individual",
        "title": None,
        "affiliation": None,
        "jurisdiction": None,
        "record_evidence": "Jane Doe",
        "endorsement_context_evidence": None,
        "support_mode": "page_or_section_context",
    }]}
    aligned, withheld, _report = align_extraction_exact_spans(
        "endorsements",
        model,
        source,
        endorsement_context_provenance=provenance,
    )
    assert withheld == []
    item = aligned["endorsements"][0]
    assert item["endorsement_context_evidence_source_scope"] == (
        "page_or_section_route_context"
    )
    assert item["endorsement_context_evidence"] is None
    assert item["endorsement_context_evidence_start_char"] is None
    assert item["endorsement_context_evidence_parent_start_char"] is None
    assert item["endorsement_context_evidence_exact_match_count"] == 0
    assert item["endorsement_context_evidence_alignment_method"] == (
        ALIGNMENT_METHOD_PAGE_ROUTE
    )
    assert item["endorsement_context_qualifying_wayback_url"] == (
        provenance["qualifying_wayback_url"]
    )
    validation = validate_extraction(
        "endorsements",
        aligned,
        source,
        "2.1",
        endorsement_context_provenance=provenance,
        parent_source_text=source,
    )
    assert validation["schema_valid"] and validation["n_supported"] == 1


@pytest.mark.parametrize(
    "unsafe_context",
    (
        "CHIP IN $25 TO SUPPORT PHIL ARBALLO →",
        "Do you believe in our campaign? Add your name and endorsement.",
        "“I support Senator Talent because he supports working people.”",
        "View All Endorsements",
        "ANTHONY D’ESPOSITO ANNOUNCES LAW ENFORCEMENT GROUP ENDORSEMENTS",
        "Become One of His Founding Supporters with a $25 Donation to Keep NC Red >>",
        "https://example.test/2020-endorsements/",
    ),
)
def test_v2_1_parent_context_rejects_ctas_testimonials_and_links(
    unsafe_context: str,
) -> None:
    source = f"{unsafe_context}\nJane Doe"
    provenance = _endorsement_context(source)
    assert provenance["source_scope"] == "page_or_section_route_context"
    assert provenance["context_text"] is None
    assert provenance["no_literal_anchor_reason"] == (
        "no_controlled_parent_structural_support_heading"
    )


@pytest.mark.parametrize(
    "safe_context",
    (
        "SUPPORT",
        "Endorsements",
        "2022 Endorsements",
        "Tweets and endorsements for Kim Klacik for Congress, in Baltimore and beyond.",
    ),
)
def test_v2_1_parent_context_accepts_controlled_structural_headings(
    safe_context: str,
) -> None:
    source = f"{safe_context}\nJane Doe"
    provenance = _endorsement_context(source)
    assert provenance["source_scope"] == "segment"
    assert provenance["context_text"] == safe_context


def test_export_carries_parent_context_into_later_endorsement_segments() -> None:
    parent = "Endorsements\n" + ("Organization Name\n" * 12)
    record = {
        "document_id": "parent-1",
        "route": "endorsements",
        "source_text": parent,
        "source_urls": [
            "https://web.archive.org/web/20200101000000/"
            "https://example.test/supporters/"
        ],
    }
    segments = segment_document(record, max_chars=80, overlap_chars=10)
    assert len(segments) > 1
    first = segments[0]["endorsement_context_provenance"]
    assert first["source_scope"] == "segment"
    later = segments[-1]["endorsement_context_provenance"]
    assert later["source_scope"] == "parent_document"
    assert later["context_text"] == "Endorsements"
    assert later["context_source_segment_id"] == segments[0]["document_id"]
    assert later["context_source_segment_start_char"] == 0
    assert later["context_local_start_char"] == 0
    assert later["qualifying_original_path"] == "/supporters/"


def test_v2_issue_requires_exact_heading_and_evidence_anchors() -> None:
    source = "Jobs\nWe support collective bargaining."
    heading_start, heading_end = _span(source, "Jobs")
    evidence_start, evidence_end = _span(source, "We support collective bargaining.")
    item = {
        "source_heading": "Jobs",
        "source_heading_start_char": heading_start,
        "source_heading_end_char": heading_end,
        "claim": "Supports collective bargaining",
        "statement_kind": "support",
        "evidence": "We support collective bargaining.",
        "evidence_start_char": evidence_start,
        "evidence_end_char": evidence_end,
    }
    result = validate_extraction("issues", {"issue_blocks": [item]}, source, "2.0")
    assert result["schema_valid"]
    assert result["n_supported"] == 1
    assert result["n_exact_anchors"] == result["n_required_anchors"] == 2

    item["evidence"] = "We support  collective bargaining."
    failed = validate_extraction("issues", {"issue_blocks": [item]}, source, "2.0")
    assert failed["schema_valid"]
    assert failed["n_supported"] == 0
    assert failed["status"] == "observed_unsupported"


def test_v2_biography_enforces_release_fields_timing_and_local_relationship() -> None:
    source = "Alice formerly served as mayor. She was raised in Davis."
    office = "formerly served as mayor"
    office_start, office_end = _span(source, office)
    office_item = {
        "field": "elected_office_experience",
        "value": "Mayor",
        "timing": "former",
        "relationship": None,
        "evidence": office,
        "evidence_start_char": office_start,
        "evidence_end_char": office_end,
    }
    valid = validate_extraction("biography", {"facts": [office_item]}, source, "2.0")
    assert valid["schema_valid"] and valid["n_supported"] == 1

    office_item["timing"] = None
    assert not validate_extraction(
        "biography", {"facts": [office_item]}, source, "2.0"
    )["schema_valid"]

    roots = "raised in Davis"
    roots_start, roots_end = _span(source, roots)
    roots_item = {
        "field": "local_roots",
        "value": "Davis",
        "timing": "former",
        "relationship": "raised in",
        "evidence": roots,
        "evidence_start_char": roots_start,
        "evidence_end_char": roots_end,
    }
    assert validate_extraction(
        "biography", {"facts": [roots_item]}, source, "2.0"
    )["schema_valid"]
    roots_item["relationship"] = "grew up in"
    assert not validate_extraction(
        "biography", {"facts": [roots_item]}, source, "2.0"
    )["schema_valid"]
    roots_item["relationship"] = "raised in"
    roots_item["relationship"] = None
    assert not validate_extraction(
        "biography", {"facts": [roots_item]}, source, "2.0"
    )["schema_valid"]
    roots_item["field"] = "family"
    assert not validate_extraction(
        "biography", {"facts": [roots_item]}, source, "2.0"
    )["schema_valid"]

    address_source = "She lives at P.O. Box 123 in Davis."
    address_start, address_end = _span(address_source, "P.O. Box 123 in Davis")
    address_item = {
        "field": "local_roots",
        "value": "P.O. Box 123",
        "timing": "current",
        "relationship": "lives at",
        "evidence": "lives at P.O. Box 123 in Davis",
        "evidence_start_char": address_start - len("lives at "),
        "evidence_end_char": address_end,
    }
    assert exact_span_is_valid(
        address_source,
        address_item["evidence"],
        address_item["evidence_start_char"],
        address_item["evidence_end_char"],
    )
    assert not validate_extraction(
        "biography", {"facts": [address_item]}, address_source, "2.0"
    )["schema_valid"]


def test_v2_endorsement_requires_context_for_bare_name_records() -> None:
    source = "Endorsements\nJane Doe, Mayor of Davis"
    context_start, context_end = _span(source, "Endorsements")
    record_start, record_end = _span(source, "Jane Doe, Mayor of Davis")
    item = {
        "endorser_name": "Jane Doe",
        "entity_type": "individual",
        "endorser_category": "government_official",
        "title": "Mayor",
        "affiliation": None,
        "jurisdiction": "Davis",
        "record_evidence": "Jane Doe, Mayor of Davis",
        "record_evidence_start_char": record_start,
        "record_evidence_end_char": record_end,
        "endorsement_context_evidence": "Endorsements",
        "endorsement_context_evidence_start_char": context_start,
        "endorsement_context_evidence_end_char": context_end,
        "support_mode": "page_or_section_context",
    }
    result = validate_extraction("endorsements", {"endorsements": [item]}, source, "2.0")
    assert result["schema_valid"] and result["n_supported"] == 1
    assert result["n_exact_anchors"] == result["n_required_anchors"] == 2
    assert "organization" not in SCHEMAS_V2["endorsements"]["properties"]["endorsements"]["items"]["properties"]

    item["endorsement_context_evidence"] = None
    item["endorsement_context_evidence_start_char"] = None
    item["endorsement_context_evidence_end_char"] = None
    failed = validate_extraction(
        "endorsements", {"endorsements": [item]}, source, "2.0"
    )
    assert not failed["schema_valid"]
    assert failed["n_supported"] == 0

    explicit_source = "Jane Doe endorses Alice."
    explicit_start, explicit_end = _span(explicit_source, explicit_source)
    explicit = {
        **item,
        "record_evidence": explicit_source,
        "record_evidence_start_char": explicit_start,
        "record_evidence_end_char": explicit_end,
        "endorsement_context_evidence": explicit_source,
        "endorsement_context_evidence_start_char": explicit_start,
        "endorsement_context_evidence_end_char": explicit_end,
        "support_mode": "explicit_text",
    }
    assert validate_extraction(
        "endorsements", {"endorsements": [explicit]}, explicit_source, "2.0"
    )["n_supported"] == 1

    bare_explicit_source = "Jane Doe"
    bare_explicit = {
        **explicit,
        "record_evidence": bare_explicit_source,
        "record_evidence_start_char": 0,
        "record_evidence_end_char": len(bare_explicit_source),
        "endorsement_context_evidence": bare_explicit_source,
        "endorsement_context_evidence_start_char": 0,
        "endorsement_context_evidence_end_char": len(bare_explicit_source),
    }
    assert not validate_extraction(
        "endorsements", {"endorsements": [bare_explicit]}, bare_explicit_source, "2.0"
    )["schema_valid"]

    mismatched = {**explicit, "entity_type": "organization"}
    assert not validate_extraction(
        "endorsements", {"endorsements": [mismatched]}, explicit_source, "2.0"
    )["schema_valid"]
    unclear_category = {**explicit, "endorser_category": "unclear"}
    assert validate_extraction(
        "endorsements", {"endorsements": [unclear_category]}, explicit_source, "2.0"
    )["schema_valid"]


def test_overlap_resolution_withholds_distinct_payloads() -> None:
    base = {
        "source_heading": None,
        "source_heading_start_char": None,
        "source_heading_end_char": None,
        "claim": "Supports unions",
        "statement_kind": "support",
        "evidence": "I support unions.",
        "evidence_start_char": 0,
        "evidence_end_char": 17,
    }
    equivalent = resolve_overlap_variants([
        {"segment_id": "b", "segment_index": 1, "item": {**base, "evidence_start_char": 3, "evidence_end_char": 20}},
        {"segment_id": "a", "segment_index": 0, "item": base},
    ])
    assert equivalent["resolution_status"] == "equivalent_overlap"
    assert equivalent["selected_variant"]["segment_id"] == "a"
    assert equivalent["resolution_method"] == "stable_min_segment_key_among_equivalent_payloads"

    conflict = resolve_overlap_variants([
        {"segment_id": "a", "item": base},
        {"segment_id": "b", "item": {**base, "statement_kind": "proposal"}},
    ])
    assert conflict["resolution_status"] == "conflict"
    assert conflict["selected_variant"] is None
    assert conflict["distinct_payload_count"] == 2


def test_v2_chunk_completion_requires_frozen_contract_identity(tmp_path: Path) -> None:
    input_path = tmp_path / "chunk_00000.jsonl.gz"
    output_path = tmp_path / "chunk_00000.out.jsonl.gz"
    source_text = ""
    _write_jsonl_gz(
        input_path,
        [{
            "document_id": "a",
            "candidate_cycle_id": "C-a",
            "route": "issues",
            "raw_text_sha256": "a" * 64,
            "source_text": source_text,
        }],
    )
    contract = get_extraction_contract("2.0")
    row = {
        "document_id": "a",
        "candidate_cycle_id": "C-a",
        "route": "issues",
        "raw_text_sha256": "a" * 64,
        "model_revision": MODEL_REVISION,
        "schema_version": "2.0",
        "prompt_sha256": contract["prompt_sha256"]["issues"],
        "json_schema_sha256": contract["json_schema_sha256"]["issues"],
        "extraction_contract_sha256": contract["extraction_contract_sha256"],
    }
    _write_jsonl_gz(output_path, [row])
    assert chunk_is_complete(input_path, output_path, schema_version="2.0")
    row["prompt_sha256"] = "0" * 64
    _write_jsonl_gz(output_path, [row])
    assert not chunk_is_complete(input_path, output_path, schema_version="2.0")


def test_v2_1_restart_identity_binds_generation_config_and_finished_output(
    tmp_path: Path,
) -> None:
    from argparse import Namespace

    input_path = tmp_path / "chunk_00000.jsonl.gz"
    output_path = tmp_path / "chunk_00000.out.jsonl.gz"
    source_text = ""
    input_row = {
        "document_id": "a",
        "candidate_cycle_id": "C-a",
        "candidate_icpsr": "10001",
        "office": "House",
        "state": "AA",
        "year": 2024,
        "party": "D",
        "source_urls": ["https://example.test/issues"],
        "route": "issues",
        "raw_text_sha256": "a" * 64,
        "source_text": source_text,
    }
    _write_jsonl_gz(input_path, [input_row])
    contract = get_extraction_contract("2.1")
    args = Namespace(
        schema_version="2.1",
        model_path=tmp_path / MODEL_REVISION,
        max_output_tokens=24_576,
        max_model_len=32_768,
        seed=20260824,
        compact_repair=False,
    )
    generation_hash = generation_config_sha256(args, contract)
    raw_output = '{"issue_blocks": []}'
    _, canonicalization = canonicalize_json_string_controls(raw_output)
    structural = validate_raw_structural_whitespace(raw_output)
    model_extraction = {"issue_blocks": []}
    aligned, withheld, alignment = align_extraction_exact_spans(
        "issues", model_extraction, source_text
    )
    assert aligned == model_extraction and withheld == []
    validation = validate_extraction(
        "issues", aligned, source_text, schema_version="2.1"
    )
    guard = pre_alignment_model_item_guard_report("issues", model_extraction)
    row = {
        "document_id": "a",
        "candidate_cycle_id": "C-a",
        "route": "issues",
        "raw_text_sha256": "a" * 64,
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
        "user_message_sha256": hashlib.sha256(
            build_user_message(input_row, source_text, "2.1").encode("utf-8")
        ).hexdigest(),
        "generation_config_sha256": generation_hash,
        "temperature": 0.0,
        "thinking_enabled": False,
        "compact_repair": False,
        "input_truncated": False,
        "input_tokens_used": 0,
        "input_chars_used": len(source_text),
        "structured_decoding_backend": STRUCTURED_DECODING_BACKEND,
        "structured_decoding_disable_any_whitespace": (
            STRUCTURED_DECODING_DISABLE_ANY_WHITESPACE
        ),
        "finish_reason": "stop",
        "stop_reason": None,
        "output_tokens": 50,
        "max_output_tokens": 24_576,
        "inference_display_escape_id": contract["inference_display_escape_id"],
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
        "route_array_item_cap": contract["route_array_item_caps"]["issues"],
        "model_item_repetition_spec_id": contract[
            "model_item_repetition_spec_id"
        ],
        "model_item_repetition_spec_sha256": contract[
            "model_item_repetition_spec_sha256"
        ],
        "raw_output": raw_output,
        "model_extraction": model_extraction,
        "model_extraction_sha256": alignment[
            "original_model_extraction_sha256"
        ],
        "extraction": aligned,
        "aligned_extraction_sha256": alignment["aligned_extraction_sha256"],
        "withheld_model_items": withheld,
        "pre_alignment_model_item_guard": guard,
        "array_item_cap_report": guard["array_item_cap_report"],
        "model_item_repetition_report": guard["model_item_repetition_report"],
        "exact_alignment": alignment,
        "validation": validation,
        "parse_status": "parsed",
        **inference_display_provenance(source_text),
        **canonicalization,
        **structural,
    }
    _write_jsonl_gz(output_path, [row])
    assert chunk_is_complete(
        input_path,
        output_path,
        schema_version="2.1",
        expected_generation_config_sha256=generation_hash,
        expected_max_output_tokens=24_576,
    )
    assert not chunk_is_complete(input_path, output_path, schema_version="2.1")
    args.seed += 1
    changed_hash = generation_config_sha256(args, contract)
    assert changed_hash != generation_hash
    assert not chunk_is_complete(
        input_path,
        output_path,
        schema_version="2.1",
        expected_generation_config_sha256=changed_hash,
        expected_max_output_tokens=24_576,
    )
    row["generation_config_sha256"] = generation_hash
    row["structured_decoding_disable_any_whitespace"] = False
    _write_jsonl_gz(output_path, [row])
    assert not chunk_is_complete(
        input_path,
        output_path,
        schema_version="2.1",
        expected_generation_config_sha256=generation_hash,
        expected_max_output_tokens=24_576,
    )
    row["structured_decoding_disable_any_whitespace"] = True
    row["structured_decoding_backend"] = "auto"
    _write_jsonl_gz(output_path, [row])
    assert not chunk_is_complete(
        input_path,
        output_path,
        schema_version="2.1",
        expected_generation_config_sha256=generation_hash,
        expected_max_output_tokens=24_576,
    )
    row["structured_decoding_backend"] = STRUCTURED_DECODING_BACKEND
    row["finish_reason"] = "length"
    _write_jsonl_gz(output_path, [row])
    assert not chunk_is_complete(
        input_path,
        output_path,
        schema_version="2.1",
        expected_generation_config_sha256=generation_hash,
        expected_max_output_tokens=24_576,
    )


def test_v2_1_engine_and_generation_identity_disable_structural_whitespace(
    tmp_path: Path,
) -> None:
    from argparse import Namespace

    args = Namespace(
        schema_version="2.1",
        model_path=tmp_path / MODEL_REVISION,
        max_output_tokens=24_576,
        max_model_len=32_768,
        gpu_memory_utilization=0.9,
        seed=20260824,
        compact_repair=False,
    )
    contract = get_extraction_contract("2.1")
    config = generation_config_payload(args, contract)
    assert config["generation_config_version"] == GENERATION_CONFIG_VERSION
    assert config["structured_decoding"] == {
        "mode": "json_schema",
        "backend": "xgrammar",
        "disable_any_whitespace": True,
        "route_json_schema_sha256": contract["json_schema_sha256"],
    }
    engine = llm_engine_kwargs(args)
    assert engine["structured_outputs_config"] == {
        "backend": "xgrammar",
        "disable_any_whitespace": True,
    }

    legacy_args = copy.copy(args)
    legacy_args.schema_version = "2.0"
    legacy_config = generation_config_payload(
        legacy_args, get_extraction_contract("2.0")
    )
    assert legacy_config["generation_config_version"] == (
        "candidate_extraction_generation_v1"
    )
    assert "backend" not in legacy_config["structured_decoding"]
    assert "structured_outputs_config" not in llm_engine_kwargs(legacy_args)


def test_strict_json_decoder_fails_closed_on_invalid_or_ambiguous_json() -> None:
    assert strict_json_loads('{"facts": []}') == {"facts": []}
    for text in (
        '{"a": 1, "a": 2}', '{"a": NaN}', '{"a": "\\q"}',
        '{"a": "raw\ttab"}', '{"a": "raw\x03control"}',
    ):
        with pytest.raises(ValueError):
            strict_json_loads(text)


def test_minimal_inference_display_is_reversible_and_preserves_layout() -> None:
    source = 'Heading\n"Quoted"\tA\x03B\\t\x7f\x85'
    display, boundaries = inference_display_escape_with_boundaries(source)
    assert display == 'Heading\n\\u0022Quoted\\u0022\\tA\\u0003B\\\\t\x7f\x85'
    assert inference_display_escape(source) == display
    assert inference_display_unescape(display) == source
    assert "\n" in display
    assert '"Quoted"' not in display
    assert "\\u0022Quoted\\u0022" in display
    assert "\t" not in display
    assert "\x03" not in display
    assert "\x7f\x85" in display
    assert len(boundaries) == len(display) + 1

    collision_source = "\t\\t"
    collision_display, collision_boundaries = (
        inference_display_escape_with_boundaries(collision_source)
    )
    assert collision_display == "\\t\\\\t"
    first = collision_display.find("\\t")
    interior = collision_display.find("\\t", first + 1)
    assert (collision_boundaries[first], collision_boundaries[first + 2]) == (0, 1)
    assert collision_boundaries[interior] is None
    with pytest.raises(ValueError, match="noncanonical"):
        inference_display_unescape("\\n")

    all_controls = "".join(chr(codepoint) for codepoint in range(0xA0)) + '\\"'
    all_display = inference_display_escape(all_controls)
    assert inference_display_unescape(all_display) == all_controls
    assert all(
        character == "\n" or ord(character) >= 0x20
        for character in all_display
    )
    assert "\x7f" in all_display and "\x80" in all_display and "\x9f" in all_display


def test_json_control_canonicalizer_is_narrow_attested_and_strict_afterward() -> None:
    raw = '{"value":"A\tB\x03C\nD"}'
    canonical, report = canonicalize_json_string_controls(raw)
    assert canonical == '{"value":"A\\tB\\u0003C\\nD"}'
    assert strict_json_loads(canonical) == {"value": "A\tB\x03C\nD"}
    assert report["json_string_control_canonicalizer_id"] == (
        JSON_STRING_CONTROL_CANONICALIZER_ID
    )
    assert report["json_string_control_canonicalizer_spec_sha256"] == (
        JSON_STRING_CONTROL_CANONICALIZER_SPEC_SHA256
    )
    assert report["json_control_canonicalization_count"] == 3
    assert report["json_control_canonicalization_codepoint_counts"] == {
        "U+0003": 1, "U+0009": 1, "U+000A": 1,
    }
    assert report["raw_output_sha256"] != report["canonical_output_sha256"]

    every_c0 = "".join(chr(codepoint) for codepoint in range(0x20))
    every_raw = '{"value":"' + every_c0 + '"}'
    every_canonical, every_report = canonicalize_json_string_controls(every_raw)
    assert strict_json_loads(every_canonical) == {"value": every_c0}
    assert every_report["json_control_canonicalization_count"] == 32
    assert set(every_report["json_control_canonicalization_codepoint_counts"]) == {
        f"U+{codepoint:04X}" for codepoint in range(0x20)
    }

    # Whitespace outside strings remains untouched; invalid syntax still fails.
    outside = '{\t\n"value": 1\r}'
    unchanged, outside_report = canonicalize_json_string_controls(outside)
    assert unchanged == outside
    assert outside_report["json_control_canonicalization_count"] == 0
    assert outside_report[
        "json_control_outside_string_permitted_whitespace_codepoint_counts"
    ] == {
        "U+0009": 1, "U+000A": 1, "U+000D": 1,
    }
    assert outside_report["json_control_outside_string_invalid_count"] == 0
    assert strict_json_loads(unchanged) == {"value": 1}
    for malformed in (
        '{"a": 1, "a": 2}',
        '{"value":"bad\\\ttab"}',
        '{\x03"value":1}',
        '{"value":"\\q"}',
    ):
        candidate, _ = canonicalize_json_string_controls(malformed)
        with pytest.raises(ValueError):
            strict_json_loads(candidate)
    invalid_outside, invalid_report = canonicalize_json_string_controls(
        '{\x03"value":1}'
    )
    assert invalid_report["json_control_outside_string_invalid_count"] == 1
    assert invalid_report[
        "json_control_outside_string_invalid_codepoint_counts"
    ] == {"U+0003": 1}
    assert invalid_report[
        "json_control_outside_string_permitted_whitespace_count"
    ] == 0
    with pytest.raises(ValueError):
        strict_json_loads(invalid_outside)


def test_raw_structural_whitespace_validator_requires_fixed_separators() -> None:
    valid = '{"a": 1, "b": [2, 3], "text": "inside:  x, y\t\n\r"}'
    report = validate_raw_structural_whitespace(valid)
    assert report == {
        "raw_structural_whitespace_validator_id": (
            RAW_STRUCTURAL_WHITESPACE_VALIDATOR_ID
        ),
        "raw_structural_whitespace_validator_spec_sha256": (
            RAW_STRUCTURAL_WHITESPACE_VALIDATOR_SPEC_SHA256
        ),
        "raw_structural_whitespace_validation_passed": True,
        "raw_structural_separator_count": 6,
        "raw_structural_separator_space_count": 6,
        "raw_structural_whitespace_violation_count": 0,
        "raw_structural_whitespace_violation_counts": {},
    }
    assert (
        RAW_STRUCTURAL_WHITESPACE_VALIDATOR_SPEC_SHA256
        == "4a03ebdf798ceab3f841b7359d59ad17f34033c2f6e677edd74551935ffd8774"
    )

    object_value = {"a": 1, "b": [2, 3]}
    invalid = {
        "compact": json.dumps(object_value, separators=(",", ":")),
        "pretty": json.dumps(object_value, indent=2),
        "leading": " " + json.dumps(object_value),
        "trailing": json.dumps(object_value) + " ",
        "tab": "\t" + json.dumps(object_value),
        "line_feed": "\n" + json.dumps(object_value),
        "carriage_return": json.dumps(object_value) + "\r",
        "double_space": json.dumps(object_value).replace(": ", ":  ", 1),
        "space_after_open": json.dumps(object_value).replace("{", "{ ", 1),
        "space_before_close": json.dumps(object_value)[:-1] + " }",
    }
    for label, raw in invalid.items():
        observed = validate_raw_structural_whitespace(raw)
        assert not observed["raw_structural_whitespace_validation_passed"], label
        assert observed["raw_structural_whitespace_violation_count"] > 0, label


def test_v2_1_restart_rejects_parseable_pretty_raw_json(tmp_path: Path) -> None:
    from argparse import Namespace

    input_path = tmp_path / "chunk_00000.jsonl.gz"
    output_path = tmp_path / "chunk_00000.out.jsonl.gz"
    _write_jsonl_gz(input_path, [{"document_id": "a"}])
    contract = get_extraction_contract("2.1")
    args = Namespace(
        schema_version="2.1",
        model_path=tmp_path / MODEL_REVISION,
        max_output_tokens=24_576,
        max_model_len=32_768,
        seed=20260824,
        compact_repair=False,
    )
    generation_hash = generation_config_sha256(args, contract)
    raw_output = json.dumps({"issue_blocks": []}, indent=2)
    _, canonicalization = canonicalize_json_string_controls(raw_output)
    row = {
        "document_id": "a",
        "route": "issues",
        "model_revision": MODEL_REVISION,
        "schema_version": "2.1",
        "prompt_sha256": contract["prompt_sha256"]["issues"],
        "json_schema_sha256": contract["json_schema_sha256"]["issues"],
        "extraction_contract_sha256": contract["extraction_contract_sha256"],
        "generation_config_sha256": generation_hash,
        "structured_decoding_backend": STRUCTURED_DECODING_BACKEND,
        "structured_decoding_disable_any_whitespace": True,
        "finish_reason": "stop",
        "output_tokens": 20,
        "max_output_tokens": 24_576,
        "inference_display_escape_id": contract["inference_display_escape_id"],
        "inference_display_escape_spec_sha256": contract[
            "inference_display_escape_spec_sha256"
        ],
        "json_string_control_canonicalizer_id": contract[
            "json_string_control_canonicalizer_id"
        ],
        "json_string_control_canonicalizer_spec_sha256": contract[
            "json_string_control_canonicalizer_spec_sha256"
        ],
        "raw_output": raw_output,
        **canonicalization,
    }
    _write_jsonl_gz(output_path, [row])
    assert strict_json_loads(raw_output) == {"issue_blocks": []}
    assert not chunk_is_complete(
        input_path,
        output_path,
        schema_version="2.1",
        expected_generation_config_sha256=generation_hash,
        expected_max_output_tokens=24_576,
    )


def test_canonicalized_control_evidence_aligns_to_unchanged_source_offsets() -> None:
    source = "Worked\tfor the city\x03."
    raw = (
        '{"facts":[{"field":"public_service_experience",'
        '"value":"Worked for the city","timing":"former",'
        '"relationship":null,"evidence":"Worked\tfor the city\x03."}]}'
    )
    canonical, report = canonicalize_json_string_controls(raw)
    parsed = strict_json_loads(canonical)
    aligned, withheld, alignment = align_extraction_exact_spans(
        "biography", parsed, source
    )
    assert report["json_control_canonicalization_count"] == 2
    assert withheld == []
    assert alignment["status"] == "complete"
    fact = aligned["facts"][0]
    assert fact["evidence"] == source
    assert fact["evidence_start_char"] == 0
    assert fact["evidence_end_char"] == len(source)
    assert fact["evidence_alignment_method"] == ALIGNMENT_METHOD_UNIQUE


def test_v2_1_user_message_labels_core_identifier_as_candidate_id() -> None:
    record = {
        "candidate_icpsr": "ICPSR-123",
        "candidate_name": "must not appear",
        "office": "House",
        "state": "CA",
        "year": 2020,
        "party": "D",
        "source_urls": ["https://example.test/page"],
    }
    message = build_user_message(record, "Page body", "2.1")
    assert message.startswith("Candidate ID: ICPSR-123\n")
    assert "Candidate:" not in message
    assert "must not appear" not in message
    with pytest.raises(ValueError, match="candidate_icpsr"):
        build_user_message({key: value for key, value in record.items()
                            if key != "candidate_icpsr"}, "Page body", "2.1")

    legacy = build_user_message(record, "Page body", "2.0")
    assert legacy.startswith("Candidate: must not appear\n")


def test_v2_1_user_message_binds_minimal_display_without_flattening_lines() -> None:
    record = {
        "candidate_icpsr": "ICPSR-123",
        "office": "House",
        "state": "CA",
        "year": 2020,
        "party": "D",
        "route": "issues",
        "source_urls": ["https://example.test/issues"],
    }
    source = 'Heading\n"Quoted"\tname\x03\\t\x7f\x85'
    message = build_user_message(record, source, "2.1")
    assert f"Inference display escape: {INFERENCE_DISPLAY_ESCAPE_ID}" in message
    assert INFERENCE_DISPLAY_ESCAPE_SPEC_SHA256 in message
    page = message.split(
        "Page text (minimal reversible source-character display):\n", 1
    )[1]
    assert page == inference_display_escape(source)
    assert page.startswith("Heading\n\\u0022Quoted\\u0022")
    assert "\t" not in page and "\x03" not in page
    assert "\\t" in page and "\\u0003" in page and "\\\\t" in page
    assert "\x7f\x85" in page


def test_v2_1_endorsement_user_message_binds_context_scope_and_url() -> None:
    source = "Organizations\nJane Doe"
    provenance = _endorsement_context(source)
    record = {
        "candidate_icpsr": "ICPSR-123",
        "office": "House",
        "state": "RI",
        "year": 2024,
        "party": "D",
        "route": "endorsements",
        "source_urls": [provenance["qualifying_wayback_url"]],
        "endorsement_context_provenance": provenance,
    }
    message = build_user_message(record, source, "2.1")
    assert "Source scope: page_or_section_route_context" in message
    assert f"Context ID: {provenance['context_id']}" in message
    assert "Exact support context: null (no literal support anchor" in message
    assert provenance["qualifying_original_path"] in message
    assert provenance["url_classifier_spec_sha256"] in message


class _CodePointTokenizer:
    def encode(self, text: str) -> list[str]:
        return list(text)


def test_trimming_preserves_an_exact_unicode_prefix_without_decode() -> None:
    text = "a🙂b’c"
    trimmed, truncated, tokens = trim_to_budget(_CodePointTokenizer(), text, 3)
    assert trimmed == "a🙂b"
    assert text.startswith(trimmed)
    assert truncated
    assert tokens == 3
    with pytest.raises(ValueError, match="nonnegative"):
        trim_to_budget(_CodePointTokenizer(), text, -1)


def test_recovery_metadata_attests_complete_shard_without_inference(
    tmp_path: Path,
) -> None:
    from argparse import Namespace

    args = Namespace(
        schema_version="2.0",
        model_path=tmp_path / MODEL_REVISION,
        max_output_tokens=8192,
        max_model_len=32768,
        seed=20260824,
        compact_repair=False,
        shard_index=3,
        shard_count=8,
    )
    payload = run_metadata_payload(
        args,
        get_extraction_contract("2.0"),
        started="2026-08-24T00:00:00+00:00",
        completed="2026-08-24T00:00:00+00:00",
        processed=0,
        parsed=0,
        schema_valid=0,
        exactly_supported=0,
        selected_documents=120,
        recovery_attestation=True,
    )
    assert run_metadata_name(3) == "run_metadata_00003.json"
    assert payload["documents_selected_for_shard"] == 120
    assert payload["outputs_complete_for_selected_shard"] is True
    assert payload["recovery_attestation_without_inference"] is True


def test_v2_1_metadata_binds_display_and_canonicalization_counts(
    tmp_path: Path,
) -> None:
    from argparse import Namespace

    args = Namespace(
        schema_version="2.1",
        model_path=tmp_path / MODEL_REVISION,
        max_output_tokens=24576,
        max_model_len=32768,
        seed=20260824,
        compact_repair=False,
        shard_index=None,
        shard_count=None,
    )
    contract = get_extraction_contract("2.1")
    payload = run_metadata_payload(
        args,
        contract,
        started="2026-08-24T00:00:00+00:00",
        completed="2026-08-24T00:01:00+00:00",
        processed=6,
        parsed=6,
        schema_valid=6,
        exactly_supported=6,
        selected_documents=6,
        recovery_attestation=False,
        json_control_canonicalized_outputs=6,
        json_control_canonicalized_characters=9,
        json_control_canonicalization_codepoint_counts={"U+0003": 3, "U+0009": 6},
    )
    assert payload["inference_display_escape_id"] == INFERENCE_DISPLAY_ESCAPE_ID
    assert payload["structured_decoding_backend"] == "xgrammar"
    assert payload["structured_decoding_disable_any_whitespace"] is True
    assert payload["generation_config"]["structured_decoding"] == {
        "mode": "json_schema",
        "backend": "xgrammar",
        "disable_any_whitespace": True,
        "route_json_schema_sha256": contract["json_schema_sha256"],
    }
    assert payload["json_string_control_canonicalizer_id"] == (
        JSON_STRING_CONTROL_CANONICALIZER_ID
    )
    assert payload["json_control_canonicalized_outputs_this_run"] == 6
    assert "json_control_canonicalized_documents_this_run" not in payload
    assert payload["json_control_canonicalized_characters_this_run"] == 9
    assert payload["json_control_canonicalization_codepoint_counts_this_run"] == {
        "U+0003": 3, "U+0009": 6,
    }
    assert payload["generation_config"][
        "json_string_control_canonicalizer_spec_sha256"
    ] == JSON_STRING_CONTROL_CANONICALIZER_SPEC_SHA256


def test_slurm_wrapper_defaults_to_frozen_v2_1_generation_contract() -> None:
    assert V2_1_MIN_OUTPUT_TOKENS == 24_576
    wrapper = (
        Path(__file__).resolve().parents[1]
        / "scripts"
        / "hive"
        / "run_candidate_extraction_pilot.sbatch"
    ).read_text()
    assert 'SCHEMA_VERSION="${SCHEMA_VERSION:-2.1}"' in wrapper
    assert 'MAX_OUTPUT_TOKENS="${MAX_OUTPUT_TOKENS:-24576}"' in wrapper
    assert '--max-output-tokens "$MAX_OUTPUT_TOKENS"' in wrapper
    assert 'SCHEMA_VERSION="${SCHEMA_VERSION:-2.0}"' not in wrapper
    assert 'MAX_OUTPUT_TOKENS:-8192' not in wrapper
