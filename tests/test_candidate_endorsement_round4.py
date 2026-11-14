from __future__ import annotations

import copy

import pytest

from src.candidate_endorsement_line_ledger import (
    RECALL40_ROUND3_LINE_DECISION_PROMPT,
    RECALL40_ROUND3_RECORD_EXTRACTION_PROMPT,
)
from src.candidate_endorsement_round4 import (
    ROUND4_DEVELOPMENT_BINDING,
    ROUND4_DEVELOPMENT_BINDING_SHA256,
    ROUND4_STAGE_A_PROMPT,
    ROUND4_STAGE_A_PROMPT_SHA256,
    ROUND4_STAGE_B_PROMPT,
    ROUND4_STAGE_B_PROMPT_SHA256,
    canonical_name_from_parts,
    normalize_name_for_match,
    normalized_source_matches,
    resolve_ordered_name_parts,
    sanitize_optional_fields,
    sha256_json,
    sha256_text,
    split_compound_individual_name_parts,
    stage_a_schema,
    stage_b_schema,
)


def _branch_by_disposition(schema: dict, disposition: str) -> dict:
    branches = schema["oneOf"]
    return next(
        branch for branch in branches
        if branch["properties"]["disposition"].get("const") == disposition
    )


def test_round4_prompts_are_byte_distinct_and_development_only() -> None:
    assert ROUND4_STAGE_A_PROMPT != RECALL40_ROUND3_LINE_DECISION_PROMPT
    assert ROUND4_STAGE_B_PROMPT != RECALL40_ROUND3_RECORD_EXTRACTION_PROMPT
    assert ROUND4_STAGE_A_PROMPT_SHA256 == sha256_text(ROUND4_STAGE_A_PROMPT)
    assert ROUND4_STAGE_B_PROMPT_SHA256 == sha256_text(ROUND4_STAGE_B_PROMPT)
    assert ROUND4_STAGE_A_PROMPT_SHA256 != sha256_text(
        RECALL40_ROUND3_LINE_DECISION_PROMPT
    )
    assert ROUND4_STAGE_B_PROMPT_SHA256 != sha256_text(
        RECALL40_ROUND3_RECORD_EXTRACTION_PROMPT
    )
    assert ROUND4_DEVELOPMENT_BINDING_SHA256 == sha256_json(
        ROUND4_DEVELOPMENT_BINDING
    )
    assert ROUND4_DEVELOPMENT_BINDING == {
        "version": "candidate_endorsement_round4_development_semantics_v1",
        "scope": "bounded_prompt_and_rule_development_only",
        "development_labels_authoritative": False,
        "human_truth_established": False,
        "production_integration_authorized": False,
        "full_expansion_authorized": False,
        "release_authorized": False,
    }
    assert "Stage A is a proposal" in ROUND4_STAGE_B_PROMPT
    assert "punctuation remains\nsignificant" in ROUND4_STAGE_B_PROMPT
    assert "Copy the complete visible identity" in ROUND4_STAGE_B_PROMPT
    assert "trailing comma, semicolon, colon, bullet" in ROUND4_STAGE_B_PROMPT
    assert "standalone identity listed within" in ROUND4_STAGE_B_PROMPT
    assert "Do not split one printed organization" in ROUND4_STAGE_B_PROMPT
    assert "at most one record for each source identity" in ROUND4_STAGE_B_PROMPT
    assert "four-part limit is not a four-word limit" in ROUND4_STAGE_B_PROMPT


def test_stage_a_schema_has_exact_target_and_padding_slots() -> None:
    schema = stage_a_schema(2)
    assert list(schema["properties"]) == [f"slot_{index:02d}" for index in range(16)]
    assert schema["required"] == list(schema["properties"])
    assert "padding" not in schema["properties"]["slot_01"]["properties"][
        "role"
    ]["enum"]
    assert schema["properties"]["slot_02"]["properties"]["role"] == {
        "type": "string", "const": "padding",
    }
    assert schema["additionalProperties"] is False


def test_stage_b_schema_uses_dispositions_and_name_parts() -> None:
    schema = stage_b_schema(2)
    assert list(schema["properties"]) == [f"slot_{index:02d}" for index in range(4)]
    target = schema["properties"]["slot_00"]
    assert {
        branch["properties"]["disposition"]["const"]
        for branch in target["oneOf"]
    } == {"records", "not_endorsement", "unclear"}

    records = _branch_by_disposition(target, "records")
    assert records["properties"]["source_mentions_fully_accounted"]["const"] is True
    assert records["properties"]["records"]["minItems"] == 1
    assert records["properties"]["records"]["maxItems"] == 32
    record_variants = records["properties"]["records"]["items"]["oneOf"]
    assert len(record_variants) == 8
    for variant in record_variants:
        assert "endorser_name" not in variant["properties"]
        assert variant["properties"]["name_parts"]["minItems"] == 1
        assert variant["properties"]["name_parts"]["maxItems"] == 4

    not_endorsement = _branch_by_disposition(target, "not_endorsement")
    unclear = _branch_by_disposition(target, "unclear")
    assert not_endorsement["properties"]["records"]["maxItems"] == 0
    assert not_endorsement["properties"][
        "source_mentions_fully_accounted"
    ]["const"] is True
    assert unclear["properties"]["records"]["maxItems"] == 0
    assert unclear["properties"][
        "source_mentions_fully_accounted"
    ]["const"] is False

    padding = schema["properties"]["slot_02"]
    assert padding["properties"]["disposition"]["const"] == "padding"
    assert padding["properties"]["records"]["maxItems"] == 0


def test_schema_builders_return_fresh_values_and_reject_invalid_counts() -> None:
    first = stage_b_schema(1)
    untouched = stage_b_schema(1)
    first["properties"]["slot_00"]["oneOf"][0]["properties"][
        "records"
    ]["maxItems"] = 999
    assert untouched["properties"]["slot_00"]["oneOf"][0]["properties"][
        "records"
    ]["maxItems"] == 32

    for builder in (stage_a_schema, stage_b_schema):
        with pytest.raises(ValueError, match="between one"):
            builder(0)
        with pytest.raises(ValueError, match="between one"):
            builder(5, slot_count=4)
        with pytest.raises(TypeError, match="integers"):
            builder(True)


def test_optional_field_sanitizer_is_exact_auditable_and_nonmutating() -> None:
    record = {
        "name_parts": ["Alice Rivera"],
        "entity_type": "individual",
        "endorser_category": "government_official",
        "title": "Mayor",
        "affiliation": "CITY COUNCIL",
        "jurisdiction": "New  York",
    }
    original = copy.deepcopy(record)
    sanitized, changes = sanitize_optional_fields(
        record,
        "Mayor Alice Rivera, City Council, New  York",
    )
    assert record == original
    assert sanitized == {
        **record,
        "affiliation": None,
    }
    assert changes == [{
        "field": "affiliation",
        "original_value": "CITY COUNCIL",
        "replacement_value": None,
        "reason": "not_exact_contiguous_visible_substring",
    }]


def test_normalized_source_matches_only_nfkc_case_and_unicode_whitespace() -> None:
    source = "Before ALICE\u00a0 \tSmith; then Ａｌｉｃｅ Smith."
    first = source.index("ALICE")
    first_end = source.index(";", first)
    second = source.index("Ａ")
    second_end = source.index(".", second)
    assert normalize_name_for_match("Ａlice\nSMITH") == "alice smith"
    assert normalized_source_matches("alice Smith", source) == [
        (first, first_end),
        (second, second_end),
    ]

    conservative = "R.Terry Hatch | William Sherer II | S0UTHWEST | Joanne"
    assert normalized_source_matches("R. Terry Hatch", conservative) == []
    assert normalized_source_matches("William Sherer, II", conservative) == []
    assert normalized_source_matches("Southwest", conservative) == []
    assert normalized_source_matches("Ann", conservative) == []


def test_ordered_name_parts_resolve_shared_surname_and_alias_spans() -> None:
    source = 'Alex And Jamie Rivera support Robert "Bob" Smith.'
    assert split_compound_individual_name_parts("Alex And Jamie Rivera") == (
        ("Alex", "Rivera"),
        ("Jamie", "Rivera"),
    )
    assert split_compound_individual_name_parts("Alex & Jamie Rivera") == (
        ("Alex", "Rivera"),
        ("Jamie", "Rivera"),
    )
    assert split_compound_individual_name_parts(
        "Alex Rivera AND Jamie Rivera"
    ) == (("Alex Rivera",), ("Jamie Rivera",))
    assert split_compound_individual_name_parts("Alex and Jamie") == ()

    alex_spans = resolve_ordered_name_parts(["Alex", "Rivera"], source)
    jamie_spans = resolve_ordered_name_parts(["Jamie", "Rivera"], source)
    alias_spans = resolve_ordered_name_parts(
        ["Robert", '"Bob"', "Smith"], source
    )
    assert [source[start:end] for start, end in alex_spans] == ["Alex", "Rivera"]
    assert [source[start:end] for start, end in jamie_spans] == ["Jamie", "Rivera"]
    assert [source[start:end] for start, end in alias_spans] == [
        "Robert", '"Bob"', "Smith",
    ]
    assert canonical_name_from_parts(["Robert", '"Bob"', "Smith"]) == (
        'Robert "Bob" Smith'
    )


def test_name_part_resolution_rejects_ocr_repair_and_more_than_four_parts() -> None:
    assert resolve_ordered_name_parts(["Southwest"], "S0UTHWEST") == []
    with pytest.raises(ValueError, match="one to four"):
        resolve_ordered_name_parts(["a", "b", "c", "d", "e"], "a b c d e")
