from __future__ import annotations

from copy import deepcopy
from dataclasses import replace
import hashlib
import inspect
import json

import pytest

import scripts.candidate_endorsement_round3_name_semantics as semantics
from scripts.candidate_endorsement_round3_name_semantics import (
    HUMAN_CHILD_ROW_FIELDS,
    PRODUCTION_ALIGNED_RECORD_FIELDS,
    ROUND3_NAME_SEMANTICS_SPEC,
    ROUND3_NAME_SEMANTICS_SPEC_SHA256,
    SourceSpan,
    adapt_aligned_record,
    build_human_truth_name_unit,
    derive_line_broken_person_name,
    human_truth_unit_fully_accounted,
    individual_name_semantic_errors,
    lint_individual_name,
)


REAL_LINE_FIXTURES = {
    "shared_surname": {
        "parent_document_id": "8bbbdb48c6018660ebd7e590ab5408201e0b51507975e0aef587f9cde5663b1c",
        "parent_source_text_sha256": "088797f8c7a0443ca2e117ec576267e04ae4c5b32f5d477f552b97044ffcf878",
        "window_start": 5613,
        "parent_line_index": 205,
        "window": "Steve & Becky\nNelson\n",
    },
    "naral_aflcio": {
        "parent_document_id": "e7de7ca792f0d4fb201733693e6d5270175d455c946fdc9474f6b1523eb79ac7",
        "parent_source_text_sha256": "dc9e36888c12d0e9cf2d0cd6b829fb3defefc7e14efb6c442972ca1b632e71eb",
        "window_start": 1416,
        "parent_line_index": 63,
        "window": "NARAL\nAFL-CIO\n",
    },
    "rick_issues": {
        "parent_document_id": "fbeb7737befb21cad791f5056c19994f0281cc1fbf3d53a38d6204a5ad5f34b5",
        "parent_source_text_sha256": "e48b51c4035eb492a158b7e155b1f97b578fa69acd4b51b9966150aa218ef48d",
        "window_start": 14453,
        "parent_line_index": 980,
        "window": "Rick Young\nIssues\n",
    },
    "schiffman_menu": {
        "parent_document_id": "35e8e64b016b40c9ac257c26a3f272d8d1575f74e89cbec4d5a1cbbdebe28956",
        "parent_source_text_sha256": "592395420e23227ed7c93e98e2126c5ffdfe7344fad804541ccf48a3492cce11",
        "window_start": 6974,
        "parent_line_index": 70,
        "window": "Steve Schiffman\nMenu\n",
    },
    "pratt_committeeman": {
        "parent_document_id": "8bbbdb48c6018660ebd7e590ab5408201e0b51507975e0aef587f9cde5663b1c",
        "parent_source_text_sha256": "088797f8c7a0443ca2e117ec576267e04ae4c5b32f5d477f552b97044ffcf878",
        "window_start": 5165,
        "parent_line_index": 182,
        "window": "Glen Pratt\nCommitteeman\n",
    },
    "law_order_kansas": {
        "parent_document_id": "57bd8f33192aabc91e455f5923212ddd69b2da07fc9497da95e4babe65e8d76d",
        "parent_source_text_sha256": "24a2e8a10919aa2c8e07b859bf11a19aa941341aa46af1f2c1ea5a881c852446",
        "window_start": 510,
        "parent_line_index": 34,
        "window": "Law & Order\nKansas\n",
    },
    "life_family_summary": {
        "parent_document_id": "15cccf56917802ca00f268762544d1e1a451e8113a3839e73d29fb900054588b",
        "parent_source_text_sha256": "6ee11a1f5e1fc687569d682927f27b0360dae3b1fe343988666f0cd859ba7d8f",
        "window_start": 103,
        "parent_line_index": 8,
        "window": "Life and Family\nSummary\n",
    },
}
REAL_LINE_FIXTURES_SHA256 = "5b46e3d0755c1696510d3a255640bab806878d71de2650a99aaf652072b9ed2a"
FROZEN_ROUND3_NAME_SEMANTICS_SPEC_SHA256 = (
    "832e45e0d9c891c0d7fa30bc161b2b2789cfe60ca996b00f110f632a735bbc2e"
)


class _StringSubclass(str):
    pass


class _DictSubclass(dict):
    pass


class _ListSubclass(list):
    pass


def _canonical_sha(value: object) -> str:
    return hashlib.sha256(json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")).hexdigest()


def _child_row(
    entity_type: str,
    name_spans: list[list[int]],
    support_spans: list[list[int]],
) -> dict[str, object]:
    return {
        "entity_type": entity_type,
        "name_spans": name_spans,
        "support_spans": support_spans,
    }


def _production_record(
    source: str,
    *,
    base: int,
    name: str,
    entity_type: str,
    record_span: tuple[int, int],
    context_span: tuple[int, int] | None,
    title: str | None = None,
    support_mode: str = "page_or_section_context",
    identity_index: int = 0,
) -> dict[str, object]:
    record_start, record_end = record_span
    context_start, context_end = context_span if context_span is not None else (None, None)
    def identity(label: str) -> str:
        return hashlib.sha256(f"{label}:{identity_index}".encode()).hexdigest()

    context_is_route = context_span is None
    category = {
        "individual": "private_individual",
        "organization": "other_organization",
        "unclear": "unclear",
    }[entity_type]
    return {
        "affiliation": None,
        "carrier_alignment_source_model_item_id": identity("carrier-model"),
        "endorsement_context_evidence": (
            source[context_start - base:context_end - base]
            if context_start is not None and context_end is not None
            else None
        ),
        "endorsement_context_evidence_alignment_method": (
            "page_or_section_route_context"
            if context_is_route
            else (
                "unique_exact_unicode_substring_v1"
                if support_mode == "explicit_text"
                else "provided_exact_segment_span"
            )
        ),
        "endorsement_context_evidence_end_char": (
            None if context_is_route else context_end - base
        ),
        "endorsement_context_evidence_exact_match_count": (
            0 if context_is_route else 1
        ),
        "endorsement_context_evidence_parent_end_char": context_end,
        "endorsement_context_evidence_parent_start_char": context_start,
        "endorsement_context_evidence_source_scope": (
            "page_or_section_route_context" if context_is_route else "segment"
        ),
        "endorsement_context_evidence_start_char": (
            None if context_is_route else context_start - base
        ),
        "endorsement_context_id": identity("context"),
        "endorsement_context_qualifying_original_path": (
            "/endorsements/" if context_is_route else None
        ),
        "endorsement_context_qualifying_original_url": (
            "https://example.test/endorsements/" if context_is_route else None
        ),
        "endorsement_context_qualifying_wayback_url": (
            "https://web.archive.org/web/20200101000000/https://example.test/endorsements/"
            if context_is_route else None
        ),
        "endorsement_context_record_distance_chars": (
            None
            if context_is_route or support_mode == "explicit_text"
            else record_start - context_end
        ),
        "endorser_category": category,
        "endorser_name": name,
        "entity_type": entity_type,
        "inventory_owner_segment_id": identity("owner-segment"),
        "jurisdiction": None,
        "line_inventory_id": identity("line-inventory"),
        "model_record_index": 0,
        "title": title,
        "record_evidence": source[record_start - base:record_end - base],
        "record_evidence_alignment_method": (
            "provided_authenticated_parent_projection_v1"
        ),
        "record_evidence_carrier_segment_id": identity("carrier-segment"),
        "record_evidence_end_char": record_end - base,
        "record_evidence_exact_match_count": 1,
        "record_evidence_parent_start_char": record_start,
        "record_evidence_parent_end_char": record_end,
        "record_evidence_start_char": record_start - base,
        "source_line_id": identity("source-line"),
        "source_model_item_id": identity("source-model"),
        "stage_a_batch_id": identity("stage-a-batch"),
        "stage_a_slot": f"slot_{identity_index:02d}",
        "stage_b_batch_id": identity("stage-b-batch"),
        "stage_b_slot": f"slot_{identity_index:02d}",
        "support_mode": support_mode,
        "target_block_alignment_method": (
            "deterministic_authenticated_record_block_projection_v1"
        ),
        "target_block_exact_match_count": 1,
    }


def _adapt(
    record: dict[str, object],
    source: str,
    *,
    base: int = 0,
    parent_document_id: str = "parent",
    technical_pass: bool = True,
):
    return adapt_aligned_record(
        record,
        stage_b_technical_pass=technical_pass,
        parent_document_id=parent_document_id,
        parent_source_text=source,
        parent_source_start_char=base,
    )


def test_frozen_spec_and_fixture_hashes() -> None:
    assert (
        ROUND3_NAME_SEMANTICS_SPEC_SHA256
        == FROZEN_ROUND3_NAME_SEMANTICS_SPEC_SHA256
    )
    assert ROUND3_NAME_SEMANTICS_SPEC_SHA256 == _canonical_sha(
        ROUND3_NAME_SEMANTICS_SPEC
    )
    assert _canonical_sha(REAL_LINE_FIXTURES) == REAL_LINE_FIXTURES_SHA256
    assert HUMAN_CHILD_ROW_FIELDS == {
        "entity_type", "name_spans", "support_spans"
    }
    assert {
        "record_evidence_parent_start_char",
        "record_evidence_parent_end_char",
        "endorsement_context_evidence_parent_start_char",
        "endorsement_context_evidence_parent_end_char",
    } <= PRODUCTION_ALIGNED_RECORD_FIELDS
    assert len(PRODUCTION_ALIGNED_RECORD_FIELDS) == 40


def test_authoritative_truth_api_has_no_prediction_or_inference_inputs() -> None:
    parameters = inspect.signature(build_human_truth_name_unit).parameters
    assert "entity_type" not in parameters
    assert "stage_a_role" not in parameters
    assert "model_records" not in parameters
    assert not hasattr(semantics, "source_truth_name_unit")
    assert not hasattr(semantics, "parse_compound_organization_names")


def test_human_rows_define_shared_surname_children_and_multiplicity() -> None:
    source = "Steve & Becky\nNelson\n"
    unit = build_human_truth_name_unit(
        source,
        [
            _child_row("individual", [[5613, 5618], [5627, 5633]], [[5613, 5633]]),
            _child_row("individual", [[5621, 5626], [5627, 5633]], [[5613, 5633]]),
        ],
        parent_document_id=REAL_LINE_FIXTURES["shared_surname"]["parent_document_id"],
        parent_source_start_char=5613,
    )
    assert unit.canonical_entity_names == ("Steve Nelson", "Becky Nelson")
    assert len(unit.children) == 2
    assert unit.children[0].name_spans == (
        SourceSpan(5613, 5618), SourceSpan(5627, 5633)
    )
    assert unit.children[0].support_spans == unit.children[1].support_spans


@pytest.mark.parametrize(
    "organization",
    [
        "BRICKLAYERS & ALLIED CRAFTWORKERS",
        "United Food & Commercial Workers",
        "Ohio Association of Public School Employees (OAPSE/AFSCME Local 4, AFL-CIO)",
        "Research & Education Association",
        "Johnson & Johnson PAC",
        "National Association of Realtors, Inc.",
    ],
)
def test_human_row_keeps_real_organization_names_whole(organization: str) -> None:
    unit = build_human_truth_name_unit(
        organization,
        [_child_row(
            "organization",
            [[0, len(organization)]],
            [[0, len(organization)]],
        )],
        parent_document_id="organization-parent",
    )
    assert len(unit.children) == 1
    assert unit.canonical_entity_names == (organization,)


def test_distinct_human_rows_define_duplicate_name_occurrences() -> None:
    source = "Alice Smith\nAlice Smith"
    unit = build_human_truth_name_unit(
        source,
        [
            _child_row("individual", [[0, 11]], [[0, 11]]),
            _child_row("individual", [[12, 23]], [[12, 23]]),
        ],
        parent_document_id="duplicate-parent",
    )
    assert unit.canonical_entity_names == ("Alice Smith", "Alice Smith")


def test_human_truth_rejects_empty_and_duplicate_occurrence_rows() -> None:
    with pytest.raises(ValueError, match="nonempty list"):
        build_human_truth_name_unit(
            "Alice Smith", [], parent_document_id="empty-parent"
        )
    duplicate_span_rows = [
        _child_row("individual", [[0, 11]], [[0, 5]]),
        _child_row("organization", [[0, 11]], [[6, 11]]),
    ]
    with pytest.raises(ValueError, match="repeat identical coordinates"):
        build_human_truth_name_unit(
            "Alice Smith",
            duplicate_span_rows,
            parent_document_id="duplicate-coordinate-parent",
        )


def test_human_truth_rejects_json_container_subclasses() -> None:
    row = _child_row("individual", [[0, 11]], [[0, 11]])
    with pytest.raises(ValueError, match="nonempty list"):
        build_human_truth_name_unit(
            "Alice Smith",
            _ListSubclass([row]),
            parent_document_id="container-parent",
        )
    with pytest.raises(ValueError, match="must be an object"):
        build_human_truth_name_unit(
            "Alice Smith",
            [_DictSubclass(row)],
            parent_document_id="container-parent",
        )
    row["name_spans"] = _ListSubclass([[0, 11]])
    with pytest.raises(ValueError, match="nonempty list"):
        build_human_truth_name_unit(
            "Alice Smith", [row], parent_document_id="container-parent"
        )

@pytest.mark.parametrize(
    ("mutator", "message"),
    [
        (lambda row: row.update(canonical_name="Alice Smith"), "exact fields"),
        (lambda row: row.pop("entity_type"), "exact fields"),
        (lambda row: row.update(entity_type="unclear"), "entity_type"),
        (lambda row: row.update(name_spans=[]), "nonempty list"),
        (lambda row: row.update(name_spans=((0, 5),)), "nonempty list"),
        (lambda row: row.update(name_spans=[[0, 5], [4, 11]]), "ordered"),
        (lambda row: row.update(name_spans=[[6, 11], [0, 5]]), "ordered"),
        (lambda row: row.update(name_spans=[[True, 5]]), "invalid coordinates"),
        (lambda row: row.update(name_spans=[[0, float("nan")]]), "nonfinite"),
        (lambda row: row.update(support_spans=[]), "nonempty list"),
        (lambda row: row.update(support_spans=[[0, 99]]), "outside"),
    ],
)
def test_human_child_schema_rejects_attacks(mutator, message: str) -> None:
    source = "Alice Smith"
    row = _child_row("individual", [[0, 5], [6, 11]], [[0, 11]])
    mutator(row)
    with pytest.raises(ValueError, match=message):
        build_human_truth_name_unit(
            source,
            [row],
            parent_document_id="human-schema-parent",
        )


def test_human_unresolved_type_is_preserved_and_cannot_score_complete() -> None:
    source = "Mystery Name"
    unit = build_human_truth_name_unit(
        source,
        [_child_row("unresolved", [[0, len(source)]], [[0, len(source)]])],
        parent_document_id="unresolved-parent",
    )
    record = _adapt(_production_record(
        source,
        base=0,
        name=source,
        entity_type="unclear",
        record_span=(0, len(source)),
        context_span=None,
    ), source, parent_document_id="unresolved-parent")
    assert unit.children[0].entity_type == "unresolved"
    assert not human_truth_unit_fully_accounted(unit, [record])


@pytest.mark.parametrize(
    ("name", "title", "candidate", "error"),
    [
        ("Neil Todreas, Professor", None, "Neil Todreas", "name_includes_title_office_or_rank"),
        ("Captain John Smith", None, "John Smith", "name_includes_title_office_or_rank"),
        ("Admiral John Smith", None, "John Smith", "name_includes_title_office_or_rank"),
        ("Professor Alice Jones", None, "Alice Jones", "name_includes_title_office_or_rank"),
        ("The Honorable Sean Shaw", None, "Sean Shaw", "name_includes_title_office_or_rank"),
        ("Honorable Alex Sink", None, "Alex Sink", "name_includes_title_office_or_rank"),
        ("Bob Huff, ret.", None, "Bob Huff", "name_includes_retired_marker"),
        ("Jim Coats, ret.", None, "Jim Coats", "name_includes_retired_marker"),
        ("Bill Foster, ret.", None, "Bill Foster", "name_includes_retired_marker"),
        (
            "Pinellas County Sheriff Jim Coats, ret.",
            "Pinellas County Sheriff",
            "Jim Coats",
            "name_includes_retired_marker",
        ),
        (
            "Mayor of St. Petersburg Bill Foster, ret.",
            "Mayor of St. Petersburg",
            "Bill Foster",
            "name_includes_retired_marker",
        ),
        ("Dr. James Balserak, MD", None, "James Balserak", "name_includes_title_office_or_rank"),
        ("Governor Ralph Northam", "Governor", "Ralph Northam", "name_repeats_exact_title"),
    ],
)
def test_title_and_affix_lint_does_not_depend_on_model_title(
    name: str,
    title: str | None,
    candidate: str,
    error: str,
) -> None:
    lint = lint_individual_name(name, exact_title=title)
    assert lint.canonical_candidate == candidate
    assert error in lint.errors


def test_organization_model_name_bypasses_individual_affix_lint() -> None:
    assert individual_name_semantic_errors(
        "Johnson & Johnson PAC",
        entity_type="organization",
        title=None,
    ) == ()


@pytest.mark.parametrize("name", ["Yo-Yo Ma", "Jane Do", "Alex Pe"])
def test_ambiguous_name_tails_are_not_misread_as_credentials(name: str) -> None:
    lint = lint_individual_name(name)
    assert lint.canonical_candidate == name
    assert "name_includes_postnominal_credentials" not in lint.errors


def test_aligned_record_adapter_uses_exact_production_parent_coordinates() -> None:
    source = "ENDORSEMENTS\nCaptain John Smith supports Alice.\n"
    record_start = source.index("Captain")
    record_end = source.index(".\n") + 1
    record = _production_record(
        source,
        base=0,
        name="Captain John Smith",
        entity_type="individual",
        record_span=(record_start, record_end),
        context_span=(0, len("ENDORSEMENTS")),
        title=None,
        support_mode="explicit_text",
    )
    adapted = _adapt(record, source)
    assert adapted.record_span == SourceSpan(record_start, record_end)
    assert adapted.context_span == SourceSpan(0, len("ENDORSEMENTS"))
    assert adapted.name_lint_errors == ("name_includes_title_office_or_rank",)


def test_aligned_record_adapter_accepts_production_all_null_context_triple() -> None:
    source = "Alice Smith"
    record = _production_record(
        source,
        base=0,
        name="Alice Smith",
        entity_type="individual",
        record_span=(0, len(source)),
        context_span=None,
    )
    adapted = _adapt(record, source)
    assert adapted.endorsement_context_evidence is None
    assert adapted.context_span is None

    bad = deepcopy(record)
    bad["endorsement_context_evidence_parent_start_char"] = 0
    with pytest.raises(ValueError, match="null evidence and coordinates"):
        _adapt(bad, source)


def test_aligned_record_adapter_accepts_production_unclear_entity_type() -> None:
    source = "LD 10"
    record = _production_record(
        source,
        base=0,
        name="LD 10",
        entity_type="unclear",
        record_span=(0, len(source)),
        context_span=None,
    )
    adapted = _adapt(record, source)
    assert adapted.entity_type == "unclear"


def test_adapter_recomputes_masked_support_mode() -> None:
    source = "Support Smith"
    record = _production_record(
        source,
        base=0,
        name=source,
        entity_type="individual",
        record_span=(0, len(source)),
        context_span=(0, len(source)),
        support_mode="explicit_text",
    )
    with pytest.raises(ValueError, match="masked record projection"):
        _adapt(record, source)


def test_adapter_requires_support_marker_in_every_literal_context() -> None:
    source = "Biography\nAlice supports Candidate"
    start = source.index("Alice")
    record = _production_record(
        source,
        base=0,
        name="Alice",
        entity_type="individual",
        record_span=(start, len(source)),
        context_span=(0, len("Biography")),
        support_mode="explicit_text",
    )
    with pytest.raises(ValueError, match="literal context lacks a support marker"):
        _adapt(record, source)


def test_adapter_accepts_parent_document_context_and_recomputes_count() -> None:
    source = "ENDORSEMENTS\nAlice Smith\n"
    start = source.index("Alice")
    record = _production_record(
        source,
        base=0,
        name="Alice Smith",
        entity_type="individual",
        record_span=(start, start + len("Alice Smith")),
        context_span=(0, len("ENDORSEMENTS")),
    )
    record.update({
        "endorsement_context_evidence_source_scope": "parent_document",
        "endorsement_context_evidence_alignment_method": "provided_exact_parent_span",
        "endorsement_context_evidence_start_char": None,
        "endorsement_context_evidence_end_char": None,
    })
    assert _adapt(record, source).context_span == SourceSpan(0, len("ENDORSEMENTS"))

    record["endorsement_context_evidence_exact_match_count"] = 2
    with pytest.raises(ValueError, match="exact-match count"):
        _adapt(record, source)


def test_route_context_urls_must_form_one_provenance_chain() -> None:
    source = "Alice Smith"
    record = _production_record(
        source,
        base=0,
        name=source,
        entity_type="individual",
        record_span=(0, len(source)),
        context_span=None,
    )
    record["endorsement_context_qualifying_original_path"] = "/other/"
    with pytest.raises(ValueError, match="URL and path disagree"):
        _adapt(record, source)

    record.update({
        "endorsement_context_qualifying_wayback_url": (
            "https://web.archive.org/web/20200101000000/https://example.test/about/"
        ),
        "endorsement_context_qualifying_original_url": "https://example.test/about/",
        "endorsement_context_qualifying_original_path": "/about/",
    })
    with pytest.raises(ValueError, match="not an endorsement-page route"):
        _adapt(record, source)

    record.update({
        "endorsement_context_qualifying_wayback_url": (
            "https://web.archive.org/web/20200101000000/not-a-url"
        ),
        "endorsement_context_qualifying_original_url": "not-a-url",
        "endorsement_context_qualifying_original_path": "not-a-url",
    })
    with pytest.raises(ValueError, match="original URL is invalid"):
        _adapt(record, source)


def test_adapter_rejects_mapping_subclass() -> None:
    source = "Alice Smith"
    record = _production_record(
        source,
        base=0,
        name=source,
        entity_type="individual",
        record_span=(0, len(source)),
        context_span=None,
    )
    with pytest.raises(ValueError, match="must be an object"):
        _adapt(_DictSubclass(record), source)


@pytest.mark.parametrize("technical_pass", [False, 1, None])
def test_adapter_requires_exact_true_containing_batch_status(
    technical_pass: object,
) -> None:
    source = "Alice Smith"
    record = _production_record(
        source,
        base=0,
        name=source,
        entity_type="individual",
        record_span=(0, len(source)),
        context_span=None,
    )
    with pytest.raises(ValueError, match="technical_pass true"):
        _adapt(record, source, technical_pass=technical_pass)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    ("mutator", "message"),
    [
        (lambda row: row.update(extra="x"), "exact fields"),
        (lambda row: row.pop("support_mode"), "exact fields"),
        (
            lambda row: row.update(record_evidence_parent_start_char=float("nan")),
            "nonfinite",
        ),
        (
            lambda row: row.update(record_evidence_parent_start_char=float("inf")),
            "nonfinite",
        ),
        (
            lambda row: row.update(record_evidence_parent_start_char=True),
            "coordinates are invalid",
        ),
        (
            lambda row: row.update(record_evidence_parent_start_char=0.0),
            "coordinates are invalid",
        ),
        (lambda row: row.update(record_evidence="forged"), "does not match"),
        (lambda row: row.update(title=""), "nonblank"),
        (lambda row: row.update(endorser_name=_StringSubclass("Alice Smith")), "nonblank"),
        (lambda row: row.update(affiliation=3), "nonblank"),
        (lambda row: row.update(source_model_item_id="0" * 63), "SHA-256"),
        (lambda row: row.update(stage_a_slot="slot_16"), "slot_00 through slot_15"),
        (lambda row: row.update(stage_b_slot="slot_08"), "slot_00 through slot_07"),
        (lambda row: row.update(model_record_index=32), "less than 32"),
        (lambda row: row.update(model_record_index=True), "nonnegative integer"),
        (lambda row: row.update(endorser_category="labor_union"), "incompatible"),
        (
            lambda row: row.update(endorsement_context_evidence_exact_match_count=1.0),
            "nonnegative integer",
        ),
        (
            lambda row: row.update(record_evidence_exact_match_count=True),
            "nonnegative integer",
        ),
        (
            lambda row: row.update(record_evidence_start_char=True),
            "local coordinates are invalid",
        ),
        (lambda row: row.update(support_mode="other"), "support_mode"),
        (
            lambda row: row.update(endorsement_context_evidence="forged"),
            "does not match",
        ),
    ],
)
def test_aligned_record_adapter_rejects_schema_and_coordinate_attacks(
    mutator,
    message: str,
) -> None:
    source = "ENDORSEMENTS\nAlice Smith\n"
    start = source.index("Alice")
    record = _production_record(
        source,
        base=0,
        name="Alice Smith",
        entity_type="individual",
        record_span=(start, start + len("Alice Smith")),
        context_span=(0, len("ENDORSEMENTS")),
    )
    mutator(record)
    with pytest.raises(ValueError, match=message):
        _adapt(record, source)


def _alice_unit_and_record(
    *,
    record_span: tuple[int, int],
    context_span: tuple[int, int],
    support_span: tuple[int, int],
):
    source = "ENDORSEMENTS\nAlice Smith supports Candidate.\n"
    name_start = source.index("Alice")
    name_end = name_start + len("Alice Smith")
    unit = build_human_truth_name_unit(
        source,
        [_child_row(
            "individual",
            [[name_start, name_end]],
            [[support_span[0], support_span[1]]],
        )],
        parent_document_id="alice-parent",
    )
    record = _adapt(_production_record(
        source,
        base=0,
        name="Alice Smith",
        entity_type="individual",
        record_span=record_span,
        context_span=context_span,
        support_mode=(
            "explicit_text" if record_span[1] > name_end
            else "page_or_section_context"
        ),
    ), source, parent_document_id="alice-parent")
    return unit, record


def test_accounting_accepts_exact_and_containing_support_carriers() -> None:
    source = "ENDORSEMENTS\nAlice Smith supports Candidate.\n"
    name_start = source.index("Alice")
    name_end = name_start + len("Alice Smith")
    unit, exact_record = _alice_unit_and_record(
        record_span=(name_start, name_end),
        context_span=(0, len("ENDORSEMENTS")),
        support_span=(name_start, name_end),
    )
    assert human_truth_unit_fully_accounted(unit, [exact_record])

    line_end = source.index(".\n") + 1
    unit, containing_record = _alice_unit_and_record(
        record_span=(name_start, line_end),
        context_span=(0, len("ENDORSEMENTS")),
        support_span=(name_start, name_end),
    )
    assert human_truth_unit_fully_accounted(unit, [containing_record])


def test_accounting_accepts_context_support_and_rejects_uncovered_support() -> None:
    source = "ENDORSEMENTS\nAlice Smith supports Candidate.\n"
    name_start = source.index("Alice")
    name_end = name_start + len("Alice Smith")
    unit, context_record = _alice_unit_and_record(
        record_span=(name_start, name_end),
        context_span=(0, len("ENDORSEMENTS")),
        support_span=(0, len("ENDORSEMENTS")),
    )
    assert human_truth_unit_fully_accounted(unit, [context_record])

    support_start = source.index("supports")
    support_end = support_start + len("supports")
    unit, uncovered_record = _alice_unit_and_record(
        record_span=(name_start, name_end),
        context_span=(0, len("ENDORSEMENTS")),
        support_span=(support_start, support_end),
    )
    assert not human_truth_unit_fully_accounted(unit, [uncovered_record])


def test_each_support_span_may_use_either_validated_carrier() -> None:
    source = "ENDORSEMENTS\nAlice Smith\n"
    name_start = source.index("Alice")
    name_end = name_start + len("Alice Smith")
    unit = build_human_truth_name_unit(
        source,
        [_child_row(
            "individual",
            [[name_start, name_end]],
            [[0, len("ENDORSEMENTS")], [name_start, name_end]],
        )],
        parent_document_id="two-carrier-parent",
    )
    record = _adapt(_production_record(
        source,
        base=0,
        name="Alice Smith",
        entity_type="individual",
        record_span=(name_start, name_end),
        context_span=(0, len("ENDORSEMENTS")),
    ), source, parent_document_id="two-carrier-parent")
    assert human_truth_unit_fully_accounted(unit, [record])


def test_accounting_rejects_dataclass_replace_laundering() -> None:
    source = "Alice Smith"
    unit = build_human_truth_name_unit(
        source,
        [_child_row("individual", [[0, len(source)]], [[0, len(source)]])],
        parent_document_id="sealed-parent",
    )
    record = _adapt(_production_record(
        source,
        base=0,
        name=source,
        entity_type="individual",
        record_span=(0, len(source)),
        context_span=None,
    ), source, parent_document_id="sealed-parent")

    with pytest.raises(TypeError, match="human truth builder"):
        human_truth_unit_fully_accounted(
            replace(unit, parent_source_n_chars=len(source) + 1), [record]
        )
    with pytest.raises(TypeError, match="production adapter"):
        human_truth_unit_fully_accounted(
            unit, [replace(record, endorser_name="Mallory")]
        )


def test_record_evidence_must_cover_every_split_name_span() -> None:
    source = "Steve & Becky\nNelson\n"
    parent_id = REAL_LINE_FIXTURES["shared_surname"]["parent_document_id"]
    rows = [
        _child_row("individual", [[5613, 5618], [5627, 5633]], [[5613, 5633]]),
        _child_row("individual", [[5621, 5626], [5627, 5633]], [[5613, 5633]]),
    ]
    unit = build_human_truth_name_unit(
        source,
        rows,
        parent_document_id=parent_id,
        parent_source_start_char=5613,
    )
    full_records = [
        _adapt(_production_record(
            source,
            base=5613,
            name=name,
            entity_type="individual",
            record_span=(5613, 5633),
            context_span=None,
            identity_index=identity_index,
        ), source, base=5613, parent_document_id=parent_id)
        for identity_index, name in enumerate(("Steve Nelson", "Becky Nelson"))
    ]
    assert human_truth_unit_fully_accounted(unit, full_records)

    short_records = [
        _adapt(_production_record(
            source,
            base=5613,
            name=name,
            entity_type="individual",
            record_span=(5613, 5626),
            context_span=None,
            identity_index=identity_index,
        ), source, base=5613, parent_document_id=parent_id)
        for identity_index, name in enumerate(("Steve Nelson", "Becky Nelson"))
    ]
    assert not human_truth_unit_fully_accounted(unit, short_records)


def test_distinct_organization_children_may_share_one_record_carrier() -> None:
    source = "US Indo-American Political Action Committee, US Impact\n"
    base = 355
    parent_id = "b1b3170b34afaffb186417824f70c3471a0a7d4fa3451bdee3bb6140d797e19d"
    unit = build_human_truth_name_unit(
        source,
        [
            _child_row("organization", [[355, 398]], [[355, 398]]),
            _child_row("organization", [[400, 409]], [[400, 409]]),
        ],
        parent_document_id=parent_id,
        parent_source_start_char=base,
    )
    records = [
        _adapt(_production_record(
            source,
            base=base,
            name=name,
            entity_type="organization",
            record_span=(355, 409),
            context_span=None,
            identity_index=identity_index,
        ), source, base=base, parent_document_id=parent_id)
        for identity_index, name in enumerate((
            "US Indo-American Political Action Committee",
            "US Impact",
        ))
    ]
    assert records[0].record_span == records[1].record_span
    assert human_truth_unit_fully_accounted(unit, records)

    second_only_first_child = _adapt(_production_record(
        source,
        base=base,
        name="US Impact",
        entity_type="organization",
        record_span=(355, 398),
        context_span=None,
    ), source, base=base, parent_document_id=parent_id)
    assert not human_truth_unit_fully_accounted(
        unit, [records[0], second_only_first_child]
    )


def test_accounting_enforces_row_multiplicity_entity_type_and_parent_binding() -> None:
    source = "Alice Smith\nAlice Smith"
    unit = build_human_truth_name_unit(
        source,
        [
            _child_row("individual", [[0, 11]], [[0, 11]]),
            _child_row("individual", [[12, 23]], [[12, 23]]),
        ],
        parent_document_id="duplicate-parent",
    )
    records = [
        _adapt(_production_record(
            source,
            base=0,
            name="Alice Smith",
            entity_type="individual",
            record_span=span,
            context_span=None,
            identity_index=index,
        ), source, parent_document_id="duplicate-parent")
        for index, span in enumerate(((0, 11), (12, 23)))
    ]
    assert not human_truth_unit_fully_accounted(unit, [records[0]])
    assert not human_truth_unit_fully_accounted(unit, [records[0], records[0]])
    assert human_truth_unit_fully_accounted(unit, records)

    wrong_type = _adapt(_production_record(
        source,
        base=0,
        name="Alice Smith",
        entity_type="organization",
        record_span=(12, 23),
        context_span=None,
        identity_index=1,
    ), source, parent_document_id="duplicate-parent")
    assert not human_truth_unit_fully_accounted(unit, [records[0], wrong_type])

    wrong_parent = _adapt(_production_record(
        source,
        base=0,
        name="Alice Smith",
        entity_type="individual",
        record_span=(12, 23),
        context_span=None,
        identity_index=1,
    ), source, parent_document_id="other-parent")
    assert not human_truth_unit_fully_accounted(unit, [records[0], wrong_parent])


def test_duplicate_name_matching_uses_augmenting_paths_and_rejects_hall_failure() -> None:
    source = "Alice Smith\nAlice Smith\n"
    unit = build_human_truth_name_unit(
        source,
        [
            _child_row("individual", [[0, 11]], [[0, 11]]),
            _child_row("individual", [[12, 23]], [[12, 23]]),
        ],
        parent_document_id="hall-parent",
    )
    broad = _adapt(_production_record(
        source,
        base=0,
        name="Alice Smith",
        entity_type="individual",
        record_span=(0, 23),
        context_span=None,
        identity_index=0,
    ), source, parent_document_id="hall-parent")
    first_only = _adapt(_production_record(
        source,
        base=0,
        name="Alice Smith",
        entity_type="individual",
        record_span=(0, 11),
        context_span=None,
        identity_index=1,
    ), source, parent_document_id="hall-parent")
    assert human_truth_unit_fully_accounted(unit, [broad, first_only])
    assert human_truth_unit_fully_accounted(unit, [first_only, broad])

    another_first_only = _adapt(_production_record(
        source,
        base=0,
        name="Alice Smith",
        entity_type="individual",
        record_span=(0, 11),
        context_span=None,
        identity_index=2,
    ), source, parent_document_id="hall-parent")
    assert not human_truth_unit_fully_accounted(
        unit, [first_only, another_first_only]
    )


def _line(
    source_line_id: str,
    text: str,
    index: int,
    start: int,
    role: str,
    owner: str | None,
) -> dict[str, object]:
    end = start + len(text)
    return {
        "source_line_id": source_line_id,
        "line_text": text,
        "line_terminator": "\n",
        "parent_line_index": index,
        "parent_start_char": start,
        "parent_end_char": end,
        "parent_full_end_char": end + 1,
        "stage_a_role": role,
        "continuation_owner_source_line_id": owner,
    }


def _forged_owned_lines(fixture: dict[str, object]) -> list[dict[str, object]]:
    first, second, _ = str(fixture["window"]).split("\n")
    start = int(fixture["window_start"])
    index = int(fixture["parent_line_index"])
    anchor = "anchor"
    return [
        _line(anchor, first, index, start, "record_anchor", None),
        _line(
            "continuation",
            second,
            index + 1,
            start + len(first) + 1,
            "record_continuation_previous",
            anchor,
        ),
    ]


def test_non_authoritative_shared_surname_proposal_handles_real_fixture() -> None:
    fixture = REAL_LINE_FIXTURES["shared_surname"]
    assert derive_line_broken_person_name(
        _forged_owned_lines(fixture),
        "anchor",
        parent_source_text=str(fixture["window"]),
        parent_source_start_char=int(fixture["window_start"]),
    ) == "Steve & Becky Nelson"


@pytest.mark.parametrize(
    "fixture_name",
    [
        "naral_aflcio",
        "rick_issues",
        "schiffman_menu",
        "pratt_committeeman",
        "law_order_kansas",
        "life_family_summary",
    ],
)
def test_real_forged_owner_false_joins_are_rejected(fixture_name: str) -> None:
    fixture = REAL_LINE_FIXTURES[fixture_name]
    assert derive_line_broken_person_name(
        _forged_owned_lines(fixture),
        "anchor",
        parent_source_text=str(fixture["window"]),
        parent_source_start_char=int(fixture["window_start"]),
    ) is None


@pytest.mark.parametrize(
    ("first", "second"),
    [
        ("Karen Jenkins", "Virginia"),
        ("Mike Huckabee –", "Sarah Palin"),
        ("Community Leader", "Airport Authority"),
        ("Mike Huckabee –", "HuckPAC"),
    ],
)
def test_synthetic_forged_owner_false_joins_are_rejected(
    first: str,
    second: str,
) -> None:
    fixture = {
        "window": f"{first}\n{second}\n",
        "window_start": 0,
        "parent_line_index": 0,
    }
    assert derive_line_broken_person_name(
        _forged_owned_lines(fixture),
        "anchor",
        parent_source_text=str(fixture["window"]),
    ) is None


def test_line_proposal_rejects_coordinate_owner_and_schema_attacks() -> None:
    fixture = REAL_LINE_FIXTURES["shared_surname"]
    lines = _forged_owned_lines(fixture)
    kwargs = {
        "parent_source_text": str(fixture["window"]),
        "parent_source_start_char": int(fixture["window_start"]),
    }

    missing = deepcopy(lines)
    missing[1].pop("parent_start_char")
    with pytest.raises(ValueError, match="exact line fields"):
        derive_line_broken_person_name(missing, "anchor", **kwargs)

    extra = deepcopy(lines)
    extra[1]["extra"] = "x"
    with pytest.raises(ValueError, match="exact line fields"):
        derive_line_broken_person_name(extra, "anchor", **kwargs)

    nonfinite = deepcopy(lines)
    nonfinite[1]["parent_start_char"] = float("nan")
    with pytest.raises(ValueError, match="nonfinite"):
        derive_line_broken_person_name(nonfinite, "anchor", **kwargs)

    gapped = deepcopy(lines)
    gapped[1]["parent_line_index"] = lines[0]["parent_line_index"] + 2
    with pytest.raises(ValueError, match="indices must be consecutive"):
        derive_line_broken_person_name(gapped, "anchor", **kwargs)

    wrong_owner = deepcopy(lines)
    wrong_owner[1]["continuation_owner_source_line_id"] = "other"
    assert derive_line_broken_person_name(
        wrong_owner, "anchor", **kwargs
    ) is None


def test_stage_a_output_ownership_binding_is_explicitly_pending() -> None:
    assert ROUND3_NAME_SEMANTICS_SPEC["stage_a_output_ownership_validation"] == (
        "pending_authenticated_stage_a_output_binding_before_any_production_use"
    )
