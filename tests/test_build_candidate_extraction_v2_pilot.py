"""Tests for the balanced plus control-character v2 extraction pilot."""
from __future__ import annotations

import gzip
import json
from pathlib import Path

import pandas as pd
import pytest

from scripts.build_candidate_extraction_v2_pilot import (
    EXACT_ANCHOR_ABLATION_BIOGRAPHY_DOCUMENT_ID,
    EXACT_ANCHOR_ABLATION_CONTROL_DOCUMENT_IDS,
    EXACT_ANCHOR_ABLATION_DENSE_ENDORSEMENT_DOCUMENT_ID,
    EXACT_ANCHOR_ABLATION_EXPLICIT_ENDORSEMENT_DOCUMENT_ID,
    EXACT_ANCHOR_ABLATION_ISSUE_DOCUMENT_ID,
    EXACT_ANCHOR_ABLATION_PARENT_IDS,
    MAX_COMPLEXITY_BIOGRAPHY_DOCUMENT_ID,
    MAX_DENSITY_ENDORSEMENT_DOCUMENT_ID,
    QUOTE_CAP_PREFLIGHT_DOCUMENT_IDS,
    QUOTE_BEARING_ENDORSEMENT_NAME_DOCUMENT_ID,
    QUOTE_BEARING_ENDORSEMENT_NAME_EVIDENCE,
    QUOTE_BEARING_ENDORSEMENT_NAME_PARENT_ID,
    QUOTE_COPULA_ISSUE_DOCUMENT_IDS,
    QUOTE_REGRESSION_ISSUE_DOCUMENT_IDS,
    QUOTE_REPETITION_FAILURE_CONTROL,
    QUOTE_REPETITION_FAILURE_DOCUMENT_ID,
    QUOTE_REPETITION_FAILURE_PARENT_ID,
    STRAIGHT_QUOTE_STRESS_GROUP,
    STRUCTURED_WHITESPACE_FAILURE_CONTROL,
    STRUCTURED_WHITESPACE_FAILURE_DOCUMENT_ID,
    STRUCTURED_WHITESPACE_FAILURE_PARENT_ID,
    build_quote_cap_preflight,
    build_selection,
    build_structured_whitespace_preflight,
    display_codec_stress_metrics,
    known_omissions,
    legacy_segment_density,
    pilot_manifest_source_metadata,
    sha256_file,
    straight_quote_stress_metrics,
    v2_1_source_row,
)


def _parent(parent_id: str, route: str, segments: int, records: int) -> dict:
    return {
        "parent_document_id": parent_id,
        "route": route,
        "candidate_cycle_id": f"C-{parent_id}",
        "candidate_icpsr": parent_id,
        "segment_count": segments,
        "legacy_parent_parse_status": "parsed",
        "legacy_record_count": records,
    }


def _fixtures() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    inventory_rows = []
    legacy_rows = []
    recall_rows = []
    precision_rows = []
    for route in ("biography", "endorsements", "issues"):
        for index in range(20):
            parent_id = f"{route}-{index:02d}"
            row = _parent(
                parent_id,
                route,
                segments=2 if index >= 12 else 1,
                records=(100 if index == 19 else (0 if 8 <= index < 12 else 1)),
            )
            inventory_rows.append({key: row[key] for key in (
                "parent_document_id", "route", "candidate_cycle_id",
                "candidate_icpsr", "segment_count",
            )})
            legacy_rows.append({key: row[key] for key in (
                "parent_document_id", "legacy_parent_parse_status", "legacy_record_count",
            )})
            legacy_rows[-1].update({
                "legacy_max_segment_record_count": row["legacy_record_count"],
                "legacy_densest_segment_id": f"segment-{parent_id}",
            })
        for index in range(5):
            recall_rows.append(
                {
                    "parent_document_id": f"{route}-{index:02d}",
                    "candidate_cycle_id": f"C-{route}-{index:02d}",
                    "route": route,
                    "all_requested_records_captured": "no" if route == "issues" and index == 0 else "yes",
                    "missed_records_json": (
                        '[{"type":"issue_statement","value":"x","evidence":"e"}]'
                        if route == "issues" and index == 0 else "[]"
                    ),
                }
            )
        precision_rows.append(
            {
                "parent_document_id": f"{route}-06",
                "route": route,
                **{field: "yes" for field in (
                    "record_present_in_page", "normalized_value_faithful",
                    "record_type_correct", "evidence_alone_supports_record",
                )},
            }
        )
    return (
        pd.DataFrame(inventory_rows),
        pd.DataFrame(legacy_rows),
        pd.DataFrame(recall_rows),
        pd.DataFrame(precision_rows),
    )


def test_selection_has_exact_balanced_groups_and_no_overlap() -> None:
    inventory, legacy, recall, precision = _fixtures()
    selection = build_selection(inventory, legacy, recall, precision)
    assert len(selection) == 36
    assert selection["parent_document_id"].is_unique
    assert selection.groupby("route").size().to_dict() == {
        "biography": 12,
        "endorsements": 12,
        "issues": 12,
    }
    assert selection["pilot_group"].value_counts().to_dict() == {
        "known_tuning": 15,
        "multi_segment_control": 9,
        "zero_result_control": 9,
        "worst_density_control": 3,
    }
    dense = selection.loc[selection["pilot_group"].eq("worst_density_control")]
    assert set(dense["parent_document_id"]) == {
        "biography-19", "endorsements-19", "issues-19",
    }
    assert set(dense["legacy_max_segment_record_count"]) == {100}


def test_known_omissions_requires_frozen_count() -> None:
    _, _, recall, _ = _fixtures()
    repeated = pd.concat([recall.iloc[[10]]] * 11, ignore_index=True)
    repeated["parent_document_id"] = [f"p-{index}" for index in range(11)]
    assert len(known_omissions(repeated)) == 11


def test_selection_is_deterministic() -> None:
    inventory, legacy, recall, precision = _fixtures()
    first = build_selection(inventory, legacy, recall, precision, seed=7)
    second = build_selection(
        inventory.sample(frac=1, random_state=4),
        legacy.sample(frac=1, random_state=5),
        recall.sample(frac=1, random_state=6),
        precision,
        seed=7,
    )
    assert first["parent_document_id"].tolist() == second["parent_document_id"].tolist()


def test_straight_quote_stress_expands_union_and_names_preserved_failure() -> None:
    inventory, legacy, recall, precision = _fixtures()
    additions = pd.DataFrame([
        {
            "parent_document_id": STRUCTURED_WHITESPACE_FAILURE_PARENT_ID,
            "route": "biography",
            "candidate_cycle_id": "H0IN01135-2022",
            "candidate_icpsr": "ICPSR-failure",
            "segment_count": 1,
            "straight_quote_segment_count": 1,
            "straight_quote_character_count": 5,
            "structured_whitespace_failure_segment_id": (
                STRUCTURED_WHITESPACE_FAILURE_DOCUMENT_ID
            ),
        },
        {
            "parent_document_id": "quote-issue",
            "route": "issues",
            "candidate_cycle_id": "C-quote",
            "candidate_icpsr": "ICPSR-quote",
            "segment_count": 1,
            "straight_quote_segment_count": 1,
            "straight_quote_character_count": 2,
        },
    ])
    inventory = pd.concat([inventory, additions], ignore_index=True)
    legacy = pd.concat([
        legacy,
        pd.DataFrame([
            {
                "parent_document_id": parent_id,
                "legacy_parent_parse_status": "parsed",
                "legacy_record_count": 1,
                "legacy_max_segment_record_count": 1,
                "legacy_densest_segment_id": f"legacy-{parent_id}",
            }
            for parent_id in additions["parent_document_id"]
        ]),
    ], ignore_index=True)

    selection = build_selection(inventory, legacy, recall, precision, seed=11)
    assert len(selection) == 38
    quote_rows = selection.loc[selection["straight_quote_whitespace_stress"]]
    assert set(quote_rows["parent_document_id"]) == {
        STRUCTURED_WHITESPACE_FAILURE_PARENT_ID,
        "quote-issue",
    }
    assert set(quote_rows["pilot_group"]) == {STRAIGHT_QUOTE_STRESS_GROUP}
    failure = selection.loc[
        selection["parent_document_id"].eq(
            STRUCTURED_WHITESPACE_FAILURE_PARENT_ID
        )
    ].iloc[0]
    assert failure["pilot_control_kind"] == STRUCTURED_WHITESPACE_FAILURE_CONTROL
    assert failure["pilot_control_segment_id"] == (
        STRUCTURED_WHITESPACE_FAILURE_DOCUMENT_ID
    )


def test_straight_quote_metric_counts_only_literal_u0022() -> None:
    metrics = straight_quote_stress_metrics('“curly” "straight" \\"escaped"')
    assert metrics == {"straight_quote_count": 4, "has_straight_quote": True}
    assert straight_quote_stress_metrics("curly ‘only’") == {
        "straight_quote_count": 0,
        "has_straight_quote": False,
    }


def test_display_stress_expands_to_all_changed_parents_and_names_controls() -> None:
    inventory, legacy, recall, precision = _fixtures()
    additions = [
        {
            "parent_document_id": "stress-dense",
            "route": "endorsements",
            "candidate_cycle_id": "C-dense",
            "candidate_icpsr": "dense",
            "segment_count": 3,
            "display_codec_changed_segment_count": 2,
            "tab_segment_count": 2,
            "tab_character_count": 40,
            "max_tab_segment_nonempty_lines": 30,
            "max_tab_segment_id": "dense-segment",
        },
        {
            "parent_document_id": "stress-small",
            "route": "endorsements",
            "candidate_cycle_id": "C-small",
            "candidate_icpsr": "small",
            "segment_count": 2,
            "display_codec_changed_segment_count": 1,
            "tab_segment_count": 1,
            "tab_character_count": 1,
            "min_inline_tab_segment_n_chars": 900,
            "min_inline_tab_segment_id": "small-segment",
            "max_tab_segment_nonempty_lines": 1,
            "max_tab_segment_id": "small-segment",
        },
        {
            "parent_document_id": "stress-u3",
            "route": "biography",
            "candidate_cycle_id": "C-u3",
            "candidate_icpsr": "u3",
            "segment_count": 1,
            "display_codec_changed_segment_count": 1,
            "u0003_segment_count": 1,
            "min_u0003_segment_n_chars": 700,
            "min_u0003_segment_id": "u3-segment",
        },
        {
            "parent_document_id": "stress-backslash",
            "route": "issues",
            "candidate_cycle_id": "C-backslash",
            "candidate_icpsr": "backslash",
            "segment_count": 1,
            "display_codec_changed_segment_count": 1,
            "backslash_segment_count": 1,
        },
    ]
    inventory = pd.concat([inventory, pd.DataFrame(additions)], ignore_index=True)
    legacy_additions = pd.DataFrame([
        {
            "parent_document_id": row["parent_document_id"],
            "legacy_parent_parse_status": "parsed",
            "legacy_record_count": 1,
            "legacy_max_segment_record_count": (
                250 if row["parent_document_id"] == "stress-dense" else 1
            ),
            "legacy_densest_segment_id": f"legacy-{row['parent_document_id']}",
        }
        for row in additions
    ])
    legacy = pd.concat([legacy, legacy_additions], ignore_index=True)

    selection = build_selection(inventory, legacy, recall, precision, seed=9)
    # Two stress parents also satisfy prior dense/multi controls, so the union
    # contains 36 + 4 - 2 distinct parents.
    assert len(selection) == 38
    stress = selection.loc[selection["display_codec_stress"]]
    assert set(stress["parent_document_id"]) == {
        "stress-dense", "stress-small", "stress-u3", "stress-backslash",
    }
    assert set(
        selection.loc[selection["pilot_control_kind"].ne(""), "pilot_control_kind"]
    ) == {"dense_tab_endorsement", "small_tab_endorsement", "u0003"}
    controls = selection.set_index("pilot_control_kind")
    assert controls.loc["dense_tab_endorsement", "parent_document_id"] == "stress-dense"
    assert controls.loc["small_tab_endorsement", "parent_document_id"] == "stress-small"
    assert controls.loc["u0003", "parent_document_id"] == "stress-u3"


def test_display_stress_scope_preserves_layout_quotes_del_and_c1() -> None:
    text = 'heading\n"quoted"\x7f\x85\\literal\tvalue\x03'
    metrics = display_codec_stress_metrics(text)
    assert metrics["changed_character_count"] == 5
    assert metrics["control_changed_character_count"] == 3
    assert metrics["non_lf_c0_count"] == 2
    assert metrics["backslash_count"] == 1
    assert metrics["tab_count"] == 1
    assert metrics["u0003_count"] == 1
    assert metrics["codepoint_counts"] == {
        "U+0003": 1, "U+0009": 1, "U+0022": 2, "U+005C": 1,
    }
    assert metrics["control_codepoint_counts"] == {
        "U+0003": 1, "U+0009": 1, "U+005C": 1,
    }


def test_segment_density_uses_single_window_max_with_stable_tie_break(
    tmp_path: Path,
) -> None:
    path = tmp_path / "legacy.jsonl.gz"
    rows = [
        {
            "parent_document_id": "p",
            "document_id": "b",
            "route": "issues",
            "extraction": {"issue_blocks": [{}, {}, {}]},
        },
        {
            "parent_document_id": "p",
            "document_id": "a",
            "route": "issues",
            "extraction": {"issue_blocks": [{}, {}, {}]},
        },
        {
            "parent_document_id": "p",
            "document_id": "c",
            "route": "issues",
            "extraction": {"issue_blocks": [{}] * 2},
        },
    ]
    with gzip.open(path, "wt", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row) + "\n")
    result = legacy_segment_density(path).iloc[0]
    assert result["legacy_max_segment_record_count"] == 3
    assert result["legacy_densest_segment_id"] == "a"


def test_v2_1_source_interface_renames_legacy_candidate_field() -> None:
    legacy = v2_1_source_row({"document_id": "d", "candidate_name": "ICPSR-1"})
    assert legacy["candidate_icpsr"] == "ICPSR-1"
    assert "candidate_name" not in legacy
    current = v2_1_source_row({"document_id": "d", "candidate_icpsr": "ICPSR-2"})
    assert current["candidate_icpsr"] == "ICPSR-2"
    with pytest.raises(ValueError, match="candidate_icpsr"):
        v2_1_source_row({"document_id": "d"})


def test_pilot_manifest_uses_importable_source_schema() -> None:
    transform = {"version": "v", "rule": "r", "offset_coordinate_system": "o"}
    context = {"version": "context-v1", "url_classifier_spec_sha256": "a" * 64}
    assert pilot_manifest_source_metadata(
        {
            "schema_version": "1.4",
            "source_text_transform": transform,
            "endorsement_context_provenance": context,
        },
        7,
    ) == {
        "schema_version": "1.4",
        "inference_schema_version": "2.1",
        "source_text_transform": {
            **transform,
            "transformed_exported_parent_documents": 7,
        },
        "endorsement_context_provenance": context,
    }
    with pytest.raises(ValueError, match="source export schema 1.4"):
        pilot_manifest_source_metadata(
            {"schema_version": "2.1-pilot-input-2.0", "source_text_transform": transform},
            7,
        )
    with pytest.raises(ValueError, match="source-text transform provenance"):
        pilot_manifest_source_metadata(
            {
                "schema_version": "1.4",
                "endorsement_context_provenance": context,
            },
            7,
        )
    with pytest.raises(ValueError, match="endorsement-context provenance"):
        pilot_manifest_source_metadata(
            {"schema_version": "1.4", "source_text_transform": transform}, 7
        )


def test_structured_whitespace_preflight_is_exact_named_source_window(
    tmp_path: Path,
) -> None:
    source_dir = tmp_path / "source"
    chunks_dir = source_dir / "chunks"
    chunks_dir.mkdir(parents=True)
    source_row = {
        "document_id": STRUCTURED_WHITESPACE_FAILURE_DOCUMENT_ID,
        "parent_document_id": STRUCTURED_WHITESPACE_FAILURE_PARENT_ID,
        "candidate_cycle_id": "H0IN01135-2022",
        "candidate_icpsr": "ICPSR-1",
        "route": "biography",
        "segment_index": 0,
        "segment_count": 1,
        "segment_start_char": 0,
        "segment_end_char": 24,
        "source_text": 'Named a "quoted title".',
        "source_text_transform_changed": False,
        "raw_text_sha256": "a" * 64,
        "segment_text_sha256": "b" * 64,
    }
    source_chunk = chunks_dir / "chunk_00000.jsonl.gz"
    with gzip.open(source_chunk, "wt", encoding="utf-8") as handle:
        handle.write(json.dumps(source_row) + "\n")
    source_manifest = {
        "schema_version": "1.4",
        "source_corpus_sha256": "c" * 64,
        "source_text_transform": {
            "version": "v",
            "rule": "r",
            "offset_coordinate_system": "unicode_code_points",
        },
        "endorsement_context_provenance": {
            "version": "context-v1",
            "url_classifier_spec_sha256": "d" * 64,
        },
        "chunks": [{
            "file": source_chunk.name,
            "records": 1,
            "sha256": sha256_file(source_chunk),
        }],
    }
    (source_dir / "manifest.json").write_text(json.dumps(source_manifest))

    output_dir = tmp_path / "preflight"
    manifest = build_structured_whitespace_preflight(source_dir, output_dir)
    assert manifest["mode"] == "v2_1_structured_whitespace_preflight"
    assert manifest["exported_records"] == 1
    assert manifest["target_runtime_policy"] == {
        "backend": "xgrammar",
        "disable_any_whitespace": True,
        "scope": "JSON structural whitespace only; string content unchanged",
    }
    assert manifest["failure_control"]["document_id"] == (
        STRUCTURED_WHITESPACE_FAILURE_DOCUMENT_ID
    )
    with gzip.open(
        output_dir / "chunks" / "chunk_00000.jsonl.gz",
        "rt",
        encoding="utf-8",
    ) as handle:
        exported = json.loads(handle.readline())
    assert exported == source_row


def test_quote_cap_preflight_has_exact_frozen_inventory_and_manifest(
    tmp_path: Path,
) -> None:
    source_dir = tmp_path / "source"
    chunks_dir = source_dir / "chunks"
    chunks_dir.mkdir(parents=True)
    ordered_ids = list(QUOTE_CAP_PREFLIGHT_DOCUMENT_IDS)
    parent_groups = [
        ("biography-parent", "biography", ordered_ids[0:2]),
        (
            EXACT_ANCHOR_ABLATION_PARENT_IDS[
                EXACT_ANCHOR_ABLATION_BIOGRAPHY_DOCUMENT_ID
            ],
            "biography",
            ordered_ids[2:4],
        ),
        ("endorsement-parent", "endorsements", ordered_ids[4:9]),
        (
            EXACT_ANCHOR_ABLATION_PARENT_IDS[
                EXACT_ANCHOR_ABLATION_EXPLICIT_ENDORSEMENT_DOCUMENT_ID
            ],
            "endorsements",
            ordered_ids[9:16],
        ),
        (
            EXACT_ANCHOR_ABLATION_PARENT_IDS[
                EXACT_ANCHOR_ABLATION_DENSE_ENDORSEMENT_DOCUMENT_ID
            ],
            "endorsements",
            ordered_ids[16:18],
        ),
        (
            QUOTE_BEARING_ENDORSEMENT_NAME_PARENT_ID,
            "endorsements",
            ordered_ids[18:19],
        ),
    ]
    issue_group_sizes = (3, 3, 1, 2, 1, 3, 1, 1, 3, 3, 1, 3)
    issue_parent_ids = (
        "copula-parent-1",
        "copula-parent-2",
        "regression-parent-1",
        "copula-parent-3",
        "regression-parent-2",
        EXACT_ANCHOR_ABLATION_PARENT_IDS[EXACT_ANCHOR_ABLATION_ISSUE_DOCUMENT_ID],
        "copula-parent-5",
        "regression-parent-3",
        "copula-parent-6",
        "copula-parent-7",
        "regression-parent-4",
        QUOTE_REPETITION_FAILURE_PARENT_ID,
    )
    start = 19
    for parent_id, group_size in zip(issue_parent_ids, issue_group_sizes):
        parent_groups.append(
            (parent_id, "issues", ordered_ids[start:start + group_size])
        )
        start += group_size
    assert start == len(ordered_ids) == 44
    assert [
        document_id
        for _parent_id, _route, document_ids in parent_groups
        for document_id in document_ids
    ] == ordered_ids

    rows = []
    for parent_id, route, document_ids in parent_groups:
        for segment_index, document_id in enumerate(document_ids):
            if document_id == EXACT_ANCHOR_ABLATION_DENSE_ENDORSEMENT_DOCUMENT_ID:
                source_text = "\n".join(
                    f"Endorser {index}" for index in range(203)
                )
            elif document_id == QUOTE_BEARING_ENDORSEMENT_NAME_DOCUMENT_ID:
                source_text = (
                    "Endorsements\n" + QUOTE_BEARING_ENDORSEMENT_NAME_EVIDENCE
                )
            elif route == "biography":
                source_text = 'Held the "public" office and served the public.'
            elif route == "endorsements":
                source_text = 'Organizations "endorse" the candidate.'
            else:
                source_text = 'The "policy" is supported.'
            row = {
                "document_id": document_id,
                "parent_document_id": parent_id,
                "candidate_cycle_id": f"C-{document_id[:8]}",
                "candidate_icpsr": f"I-{document_id[:8]}",
                "route": route,
                "segment_index": segment_index,
                "segment_count": len(document_ids),
                "segment_start_char": segment_index * len(source_text),
                "segment_end_char": (segment_index + 1) * len(source_text),
                "source_text": source_text,
                "source_text_transform_changed": False,
                "raw_text_sha256": "a" * 64,
                "segment_text_sha256": "b" * 64,
            }
            if document_id in {
                EXACT_ANCHOR_ABLATION_EXPLICIT_ENDORSEMENT_DOCUMENT_ID,
                EXACT_ANCHOR_ABLATION_DENSE_ENDORSEMENT_DOCUMENT_ID,
                QUOTE_BEARING_ENDORSEMENT_NAME_DOCUMENT_ID,
            }:
                row["endorsement_context_provenance"] = {
                    "source_scope": "segment",
                    "context_text": (
                        "Endorsements"
                        if document_id
                        == QUOTE_BEARING_ENDORSEMENT_NAME_DOCUMENT_ID
                        else None
                    ),
                }
            rows.append(row)
    source_chunk = chunks_dir / "chunk_00000.jsonl.gz"
    with gzip.open(source_chunk, "wt", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")
    source_manifest = {
        "schema_version": "1.4",
        "source_corpus_sha256": "c" * 64,
        "source_text_transform": {
            "version": "v",
            "rule": "r",
            "offset_coordinate_system": "unicode_code_points",
        },
        "endorsement_context_provenance": {
            "version": "context-v1",
            "url_classifier_spec_sha256": "d" * 64,
        },
        "chunks": [{
            "file": source_chunk.name,
            "records": len(rows),
            "sha256": sha256_file(source_chunk),
        }],
    }
    (source_dir / "manifest.json").write_text(json.dumps(source_manifest))

    output_dir = tmp_path / "preflight"
    manifest = build_quote_cap_preflight(source_dir, output_dir)
    assert manifest["mode"] == "v2_1_quote_cap_repetition_preflight"
    assert manifest["exported_records"] == 44
    assert manifest["parent_documents"] == 18
    assert manifest["records_by_route"] == {
        "biography": 4,
        "endorsements": 15,
        "issues": 25,
    }
    assert manifest["expected_control_ids"] == list(
        QUOTE_CAP_PREFLIGHT_DOCUMENT_IDS
    )
    assert manifest["control_kind_counts"] == {
        "dense_segment_context_endorsement": 1,
        "exact_anchor_parent_closure": 9,
        "issue_boundary_parent_coverage": 1,
        "max_complexity_biography": 1,
        "max_density_endorsement": 1,
        "preflight_parent_closure": 11,
        "quote_bearing_biography_evidence": 1,
        "quote_bearing_endorsement_name_evidence": 1,
        "quote_copula_peer": 12,
        "quote_escape_regression": 4,
        QUOTE_REPETITION_FAILURE_CONTROL: 1,
        "testimonial_separate_speaker_page_context": 1,
    }
    ablation = manifest["exact_copy_anchor_guidance_ablation"]
    assert ablation["all_selected_parents_complete"] is True
    assert ablation["parent_level_zero_or_coverage_interpretation_allowed"] is True
    assert set(ablation["named_control_document_ids"]) == set(
        EXACT_ANCHOR_ABLATION_CONTROL_DOCUMENT_IDS
    )
    dense_correction = manifest["endorsement_dense_recall_correction"]
    assert dense_correction["guided_schema_max_items"] == {
        "issues": 128,
        "biography": 96,
        "endorsements": None,
    }
    assert dense_correction["post_generation_caps_unchanged"] == {
        "issues": 128,
        "biography": 96,
        "endorsements": 256,
    }
    assert dense_correction["exact_text_scope_continuation_instruction"] is True
    assert dense_correction[
        "segment_has_complete_line_with_nullable_source_parent_and_display_bounds"
    ] is True
    assert dense_correction[
        "operative_prompt_bounds_use_inference_display_coordinates"
    ] is True
    assert dense_correction["restart_binds_exact_user_message_sha256"] is True
    assert manifest["superseded_preflight_no_go"] == {
        "job_id": "21424457",
        "technical_gate_passed": True,
        "endorsement_recall_failure_document_ids": [
            MAX_DENSITY_ENDORSEMENT_DOCUMENT_ID,
            "2e9a0cd8e0bd57af5f8b1b8cd26e06fa8d647d845d8ddc18a8b4199a4e666e73",
            "e56c1b40a3bc139cc9ac53e347673ab061bdc26da15a4333e928571aeff4eb32",
        ],
        "stress_rerun_submitted": False,
    }
    selection = pd.read_csv(output_dir / "selection.csv", dtype=str)
    assert set(selection["document_id"]) == set(QUOTE_CAP_PREFLIGHT_DOCUMENT_IDS)
    assert selection["document_id"].is_unique
    assert selection.loc[
        selection["document_id"] == QUOTE_REPETITION_FAILURE_DOCUMENT_ID,
        "pilot_control_kind",
    ].tolist() == [QUOTE_REPETITION_FAILURE_CONTROL]
    for document_id in (
        *EXACT_ANCHOR_ABLATION_CONTROL_DOCUMENT_IDS,
        QUOTE_BEARING_ENDORSEMENT_NAME_DOCUMENT_ID,
    ):
        assert selection.loc[
            selection["document_id"] == document_id,
            "pilot_control_kind",
        ].str.len().gt(0).all()

    with gzip.open(
        output_dir / "chunks" / "chunk_00000.jsonl.gz",
        "rt",
        encoding="utf-8",
    ) as handle:
        exported_rows = [json.loads(line) for line in handle if line.strip()]
    for parent_id in {str(row["parent_document_id"]) for row in exported_rows}:
        parent_rows = [
            row for row in exported_rows
            if str(row["parent_document_id"]) == parent_id
        ]
        assert sorted(int(row["segment_index"]) for row in parent_rows) == list(
            range(int(parent_rows[0]["segment_count"]))
        )
