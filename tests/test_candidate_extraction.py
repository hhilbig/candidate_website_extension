"""Focused tests for CLARA-style candidate-page extraction."""
from __future__ import annotations

import gzip
import json
from pathlib import Path

import pandas as pd

from scripts.build_candidate_extraction_validation import (
    build_validation_files,
    source_context,
)
from scripts.export_candidate_extraction import (
    deduplicate_target_pages,
    file_sha256,
    sha256_text,
    segment_document,
    segment_documents,
    select_stratified_pilot,
    write_chunks,
)
from scripts.hive.extract_candidate_pages_vllm import (
    COMPACT_REPAIR_RULE,
    chunk_is_complete,
    select_shard,
)
from scripts.import_candidate_extraction import (
    aggregate_parent_extractions,
    flatten_parent_records,
    import_results,
)
from scripts.merge_candidate_extraction_validation import (
    merge_precision_rows,
    merge_recall_rows,
)
from scripts.repair_candidate_extraction import (
    apply_repairs,
    collapse_subsegments,
    prepare,
    prepare_quote_normalized,
    prepare_subsegments,
    reanchor_normalized_evidence,
    read_rows,
)
from scripts.score_candidate_extraction_validation import score_validation
from src.candidate_extraction import (
    COMMON_RULES,
    MODEL_ID,
    MODEL_REVISION,
    RUNTIME_LOCK_SHA256,
    evidence_is_supported,
    validate_extraction,
)


def test_prompt_requires_contiguous_verbatim_evidence_and_complete_json():
    assert "exact, contiguous span" in COMMON_RULES
    assert "Never join separate passages" in COMMON_RULES
    assert "valid, complete JSON" in COMMON_RULES
    assert "at most 240 characters" in COMPACT_REPAIR_RULE
    assert "exact contiguous substring" in COMPACT_REPAIR_RULE
    assert "no quotation-mark character" in COMPACT_REPAIR_RULE


def _raw_row(
    candidate: str,
    route: str,
    text: str,
    date: str,
    *,
    year: int = 2020,
    party: str = "D",
) -> dict:
    return {
        "candidate_cycle_id": f"{candidate}-{year}",
        "candidate_icpsr": f"Candidate, {candidate}",
        "state": "AA",
        "office": "house",
        "year": year,
        "party": party,
        "date": date,
        "snap_url": f"https://web.archive.org/web/{date}/https://example.org/{route}",
        "page_type": route,
        "text_snap_content": text,
    }


def _write_gzip_jsonl(path: Path, rows: list[dict]) -> None:
    with gzip.open(path, "wt", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row) + "\n")


def test_deduplication_preserves_capture_bounds_and_route() -> None:
    frame = pd.DataFrame([
        _raw_row("H0AA00001", "issues", "Jobs#+#We support good jobs.", "20200102010101"),
        _raw_row("H0AA00001", "issues", "Jobs#+#We support good jobs.", "20200304010101"),
        _raw_row("H0AA00001", "biography", "Alice grew up locally.", "20200102010101"),
        _raw_row("H0AA00001", "homepage", "Not routed.", "20200102010101"),
    ])
    records = deduplicate_target_pages(frame)
    assert len(records) == 2
    issue = next(row for row in records if row["route"] == "issues")
    assert issue["n_raw_occurrences"] == 2
    assert issue["first_capture_timestamp"] == "20200102010101"
    assert issue["last_capture_timestamp"] == "20200304010101"
    assert issue["source_text"] == "Jobs\nWe support good jobs."
    assert issue["raw_text_sha256"] != issue["extraction_source_text_sha256"]
    assert issue["extraction_source_text_sha256"] == sha256_text(issue["source_text"])
    assert issue["extraction_source_n_chars"] == len(issue["source_text"])
    assert len(issue["document_id"]) == 64


def test_stratified_pilot_is_deterministic_and_balanced() -> None:
    records = []
    for route in ("issues", "biography", "endorsements"):
        for index in range(8):
            records.append({
                "document_id": f"{route}-{index}",
                "route": route,
                "year": 2020 + 2 * (index % 2),
                "party": "D" if index % 2 == 0 else "R",
            })
    first = select_stratified_pilot(records, per_route=4, seed=20260822)
    second = select_stratified_pilot(records, per_route=4, seed=20260822)
    assert first == second
    counts = pd.Series([row["route"] for row in first]).value_counts().to_dict()
    assert counts == {"issues": 4, "biography": 4, "endorsements": 4}
    assert len({row["document_id"] for row in first}) == 12


def test_segmentation_is_overlapping_exact_and_reconstructable() -> None:
    text = "first paragraph\n" + "x" * 25 + "\nlast paragraph"
    record = {"document_id": "parent", "route": "issues", "source_text": text}
    segments = segment_document(record, max_chars=24, overlap_chars=6)
    assert len(segments) > 1
    assert all(row["parent_document_id"] == "parent" for row in segments)
    assert len({row["document_id"] for row in segments}) == len(segments)
    for row in segments:
        start, end = row["segment_start_char"], row["segment_end_char"]
        assert row["source_text"] == text[start:end]
    assert segments[1]["segment_start_char"] < segments[0]["segment_end_char"]


def test_segmentation_supports_route_specific_window_sizes() -> None:
    records = [
        {"document_id": "issues", "route": "issues", "source_text": "x" * 40},
        {"document_id": "endorsers", "route": "endorsements", "source_text": "x" * 40},
    ]
    segments = segment_documents(
        records, max_chars=30, overlap_chars=3,
        route_overrides={"endorsements": (15, 2)},
    )
    counts = pd.Series(row["route"] for row in segments).value_counts().to_dict()
    assert counts["issues"] == 2
    assert counts["endorsements"] == 3


def test_evidence_validation_requires_source_support() -> None:
    source = "Alice served as mayor for eight years and graduated from State College."
    extraction = {
        "facts": [{
            "field": "prior_elected_office",
            "value": "Mayor for eight years",
            "evidence": "served as mayor for eight years",
        }]
    }
    result = validate_extraction("biography", extraction, source)
    assert result == {
        "schema_valid": True,
        "n_items": 1,
        "n_supported": 1,
        "evidence_support_rate": 1.0,
        "status": "observed_supported",
    }
    extraction["facts"][0]["evidence"] = "served in Congress"
    result = validate_extraction("biography", extraction, source)
    assert result["status"] == "observed_unsupported"
    assert result["n_supported"] == 0
    assert evidence_is_supported("graduated  from State College", source)


def test_validation_context_locates_whitespace_and_typographic_variants() -> None:
    source = "prefix " * 100 + "and was\nDeputy Director of ASU’s Center" + " suffix" * 100
    evidence = "and was Deputy Director of ASU's Center"
    context = source_context(source, evidence, radius=30)
    assert "and was\nDeputy Director of ASU’s Center" in context
    assert not context.startswith("prefix prefix")


def test_chat_validation_merge_requires_complete_exact_samples() -> None:
    precision = [{"validation_id": "p1", "coding_status": "pending"}]
    precision_results = {
        "n_complete": 1,
        "responses": [{
            "item_id": "p1",
            "status": "complete",
            "answers": {
                "record_present_in_page": "yes",
                "normalized_value_faithful": "no",
                "record_type_correct": "yes",
                "evidence_alone_supports_record": "no",
                "notes": "check",
            },
        }],
    }
    merged_precision = merge_precision_rows(precision, precision_results)
    assert merged_precision[0]["coding_status"] == "complete"
    assert merged_precision[0]["normalized_value_faithful"] == "no"

    recall = [{"parent_document_id": "r1", "coding_status": "pending"}]
    recall_results = {
        "n_complete": 1,
        "coding_mode": "ai_assisted_audit",
        "responses": [{
            "item_id": "r1",
            "status": "complete",
            "answers": {
                "all_requested_records_captured": "no",
                "missed_records": [{"type": "issue_statement", "value": "x"}],
                "notes": "missed",
            },
        }],
    }
    merged_recall = merge_recall_rows(recall, recall_results)
    assert merged_recall[0]["all_requested_records_captured"] == "no"
    assert json.loads(merged_recall[0]["missed_records_json"])[0]["value"] == "x"


def test_candidate_extraction_release_scope_records_review_decisions() -> None:
    scope = json.loads(
        Path("config/candidate_extraction_release_scope.json").read_text()
    )
    assert scope["biography_fields_excluded_from_release"] == ["family", "other"]
    elected = scope["biography_field_release_mapping"]["prior_elected_office"]
    public = scope["biography_field_release_mapping"]["prior_public_service"]
    assert elected["release_field"] == "elected_office_experience"
    assert elected["allowed_timing_values"] == ["current", "former", "unclear"]
    assert public["release_field"] == "public_service_experience"
    assert public["allowed_timing_values"] == ["current", "former", "unclear"]
    assert scope["endorsement_release_scope"]["withheld_fields"] == ["organization"]


def test_chunk_completion_checks_exact_ids_and_model_revision(tmp_path: Path) -> None:
    input_path = tmp_path / "chunk_00000.jsonl.gz"
    output_path = tmp_path / "chunk_00000.out.jsonl.gz"
    _write_gzip_jsonl(input_path, [{"document_id": "a"}, {"document_id": "b"}])
    _write_gzip_jsonl(output_path, [
        {"document_id": "a", "model_revision": MODEL_REVISION},
        {"document_id": "b", "model_revision": MODEL_REVISION},
    ])
    assert chunk_is_complete(input_path, output_path)
    _write_gzip_jsonl(output_path, [
        {"document_id": "b", "model_revision": MODEL_REVISION},
        {"document_id": "a", "model_revision": MODEL_REVISION},
    ])
    assert not chunk_is_complete(input_path, output_path)


def test_chunk_shards_are_disjoint_and_complete() -> None:
    chunks = [Path(f"chunk_{index:05d}.jsonl.gz") for index in range(11)]
    shards = [select_shard(chunks, index, 4) for index in range(4)]
    assert sorted(path for shard in shards for path in shard) == chunks
    assert not ({*shards[0]} & {*shards[1]})
    assert select_shard(chunks, None, None) == chunks


def test_importer_enforces_complete_identity_and_evidence(tmp_path: Path) -> None:
    export_dir = tmp_path / "export"
    chunks_dir = export_dir / "chunks"
    record = {
        "document_id": "doc-1",
        "candidate_cycle_id": "H0AA00001-2020",
        "candidate_name": "Candidate, Alice",
        "state": "AA",
        "office": "house",
        "year": 2020,
        "party": "D",
        "route": "issues",
        "raw_text_sha256": "a" * 64,
        "source_text": "Jobs. We support good jobs.",
        "n_raw_occurrences": 1,
        "first_capture_timestamp": "20200102010101",
        "last_capture_timestamp": "20200102010101",
        "source_urls": ["https://example.org/issues"],
    }
    chunks = write_chunks([record], chunks_dir, chunk_size=1)
    export_dir.joinpath("manifest.json").write_text(json.dumps({
        "exported_records": 1,
        "chunks": chunks,
    }))

    hive_dir = tmp_path / "hive"
    hive_dir.mkdir()
    _write_gzip_jsonl(hive_dir / "chunk_00000.out.jsonl.gz", [{
        "document_id": "doc-1",
        "candidate_cycle_id": "H0AA00001-2020",
        "route": "issues",
        "raw_text_sha256": "a" * 64,
        "model_id": MODEL_ID,
        "model_revision": MODEL_REVISION,
        "runtime_lock_sha256": RUNTIME_LOCK_SHA256,
        "parse_status": "parsed",
        "input_truncated": False,
        "input_tokens_used": 10,
        "input_chars_used": 28,
        "extraction": {
            "issue_blocks": [{
                "heading": "Jobs",
                "claim": "Supports good jobs",
                "evidence": "We support good jobs.",
            }]
        },
    }])
    enriched, summary, report = import_results(export_dir, hive_dir)
    assert len(enriched) == len(summary) == 1
    assert report["technical_gate_passed"]
    assert report["evidence_support_rate"] == 1.0
    assert report["repair_documents"] == 0
    assert report["substantive_validation_status"] == "pending_human_review"
    assert file_sha256(chunks_dir / chunks[0]["file"]) == chunks[0]["sha256"]


def test_parent_aggregation_retains_overlap_variants_and_failures() -> None:
    base = {
        "parent_document_id": "parent-1",
        "candidate_cycle_id": "H0AA00001-2020",
        "candidate_name": "Candidate, Alice",
        "state": "AA",
        "office": "house",
        "year": 2020,
        "party": "D",
        "route": "issues",
        "raw_text_sha256": "a" * 64,
        "source_text": "Jobs. We support good jobs.",
    }
    item = {"heading": "Jobs", "claim": "Supports good jobs", "evidence": "We support good jobs."}
    rows = [
        {**base, "document_id": "segment-0", "segment_index": 0,
         "parse_status": "parsed", "extraction": {"issue_blocks": [item]}},
        {**base, "document_id": "segment-1", "segment_index": 1,
         "parse_status": "parsed", "extraction": {"issue_blocks": [item]}},
        {**base, "document_id": "segment-2", "segment_index": 2,
         "parse_status": "json_parse_error", "extraction": None},
    ]
    parents = aggregate_parent_extractions(rows)
    assert len(parents) == 1
    assert parents[0]["parent_parse_status"] == "partially_parsed"
    assert parents[0]["parsed_segment_count"] == 2
    assert len(parents[0]["records"]) == 1
    assert len(parents[0]["records"][0]["variants"]) == 2
    flat = flatten_parent_records(parents)["issues"]
    assert len(flat) == 1
    assert flat["record_id"].is_unique
    assert bool(flat.iloc[0]["representative_evidence_supported"])


def test_validation_scorer_requires_complete_binary_coding() -> None:
    precision = pd.DataFrame([{
        "route": "issues",
        "subtype": "issue_statement",
        "record_present_in_page": "yes",
        "normalized_value_faithful": "yes",
        "record_type_correct": "yes",
        "evidence_alone_supports_record": "no",
    }])
    recall = pd.DataFrame([{"all_requested_records_captured": "yes"}])
    report = score_validation(precision, recall)
    assert report["record_all_dimensions_correct"]["rate"] == 0.0
    assert report["precision_dimensions"]["record_present_in_page"]["rate"] == 1.0
    assert report["pages_with_no_missed_records"]["rate"] == 1.0
    precision.loc[0, "record_type_correct"] = ""
    try:
        score_validation(precision, recall)
    except ValueError as error:
        assert "incomplete or invalid" in str(error)
    else:
        raise AssertionError("incomplete coding should fail closed")


def test_validation_population_deduplicates_overlap_record_variants(tmp_path: Path) -> None:
    imported = tmp_path / "segments.jsonl.gz"
    base = {
        "parent_document_id": "parent",
        "candidate_cycle_id": "H0AA00001-2020",
        "candidate_name": "Candidate, Alice",
        "year": 2020,
        "party": "D",
        "route": "issues",
        "parse_status": "parsed",
        "segment_count": 2,
    }
    item_a = {"heading": "Jobs", "claim": "Supports good jobs", "evidence": "We support good jobs."}
    item_b = {"heading": "Jobs", "claim": "Supports good jobs", "evidence": "support good jobs"}
    _write_gzip_jsonl(imported, [
        {**base, "document_id": "s0", "segment_index": 0,
         "segment_start_char": 0, "segment_end_char": 27,
         "source_text": "Jobs. We support good jobs.",
         "extraction": {"issue_blocks": [item_a]}},
        {**base, "document_id": "s1", "segment_index": 1,
         "segment_start_char": 6, "segment_end_char": 27,
         "source_text": "We support good jobs.",
         "extraction": {"issue_blocks": [item_b]}},
    ])
    precision, recall, _ = build_validation_files(imported, 5, 1, 20260822)
    assert len(precision) == 1
    assert precision.iloc[0]["model_variant_count"] == 2
    assert len(recall) == 1


def test_parse_error_repair_is_prepared_and_applied_atomically(tmp_path: Path) -> None:
    export_dir = tmp_path / "export"
    chunks_dir = export_dir / "chunks"
    chunks_dir.mkdir(parents=True)
    source = {
        "document_id": "doc-1",
        "candidate_cycle_id": "H0AA00001-2020",
        "route": "issues",
        "raw_text_sha256": "a" * 64,
        "source_text": "We support good jobs.",
    }
    input_path = chunks_dir / "chunk_00000.jsonl.gz"
    _write_gzip_jsonl(input_path, [source])
    export_dir.joinpath("manifest.json").write_text(json.dumps({
        "exported_records": 1,
        "chunks": [{
            "file": input_path.name,
            "sha256": file_sha256(input_path),
        }],
    }))
    output_dir = tmp_path / "outputs"
    output_dir.mkdir()
    failed = {
        **{key: source[key] for key in (
            "document_id", "candidate_cycle_id", "route", "raw_text_sha256"
        )},
        "model_revision": MODEL_REVISION,
        "runtime_lock_sha256": RUNTIME_LOCK_SHA256,
        "parse_status": "json_parse_error",
    }
    original_path = output_dir / "chunk_00000.out.jsonl.gz"
    _write_gzip_jsonl(original_path, [failed])

    repair_dir = tmp_path / "repair"
    assert prepare(export_dir, output_dir, repair_dir) == 1
    assert read_rows(repair_dir / "chunks" / "chunk_00000.jsonl.gz") == [source]

    repair_outputs = tmp_path / "repair_outputs"
    repair_outputs.mkdir()
    repaired = {
        **failed,
        "parse_status": "parsed",
        "extraction": {"issue_blocks": []},
    }
    _write_gzip_jsonl(repair_outputs / "chunk_00000.out.jsonl.gz", [repaired])
    assert apply_repairs(output_dir, repair_outputs) == 1
    rows = read_rows(original_path)
    assert rows[0]["parse_status"] == "parsed"


def test_subsegment_repair_collapses_supported_overlap_records(tmp_path: Path) -> None:
    export_dir = tmp_path / "export"
    chunks = export_dir / "chunks"
    chunks.mkdir(parents=True)
    source = {
        "document_id": "parent",
        "candidate_cycle_id": "H0AA00001-2020",
        "route": "issues",
        "raw_text_sha256": "a" * 64,
        "source_text": "Jobs. We support good jobs. " * 40,
    }
    input_path = chunks / "chunk_00000.jsonl.gz"
    _write_gzip_jsonl(input_path, [source])
    export_dir.joinpath("manifest.json").write_text(json.dumps({
        "exported_records": 1,
        "chunks": [{"file": input_path.name, "sha256": file_sha256(input_path)}],
    }))
    outputs = tmp_path / "outputs"
    outputs.mkdir()
    _write_gzip_jsonl(outputs / "chunk_00000.out.jsonl.gz", [{
        "document_id": "parent", "parse_status": "json_parse_error"
    }])
    repair = tmp_path / "repair"
    assert prepare_subsegments(export_dir, outputs, repair, 500, 100) == 3
    repair_inputs = read_rows(repair / "chunks" / "chunk_00000.jsonl.gz")
    repair_outputs = tmp_path / "repair_outputs"
    repair_outputs.mkdir()
    item = {"heading": "Jobs", "claim": "Supports good jobs", "evidence": "We support good jobs."}
    rows = []
    for row in repair_inputs:
        rows.append({
            "document_id": row["document_id"],
            "candidate_cycle_id": row["candidate_cycle_id"],
            "route": "issues",
            "raw_text_sha256": row["raw_text_sha256"],
            "model_revision": MODEL_REVISION,
            "runtime_lock_sha256": RUNTIME_LOCK_SHA256,
            "parse_status": "parsed",
            "extraction": {"issue_blocks": [item]},
        })
    _write_gzip_jsonl(repair_outputs / "chunk_00000.out.jsonl.gz", rows)
    collapsed_path = tmp_path / "collapsed" / "chunk_00000.out.jsonl.gz"
    assert collapse_subsegments(repair, repair_outputs, collapsed_path) == 1
    collapsed = read_rows(collapsed_path)
    assert collapsed[0]["document_id"] == "parent"
    assert collapsed[0]["repair_segment_count"] == 3
    assert collapsed[0]["repair_depth"] == 1
    assert collapsed[0]["extraction"]["issue_blocks"] == [item]


def test_quote_normalized_repair_preserves_mapping_and_removes_quotes(tmp_path: Path) -> None:
    export_dir = tmp_path / "export"
    chunks = export_dir / "chunks"
    chunks.mkdir(parents=True)
    source = {
        "document_id": "parent",
        "candidate_cycle_id": "H0AA00001-2020",
        "route": "issues",
        "raw_text_sha256": "a" * 64,
        "source_text": 'The “quoted title" supports schools.',
    }
    input_path = chunks / "chunk_00000.jsonl.gz"
    _write_gzip_jsonl(input_path, [source])
    export_dir.joinpath("manifest.json").write_text(json.dumps({
        "exported_records": 1,
        "chunks": [{"file": input_path.name, "sha256": file_sha256(input_path)}],
    }))
    outputs = tmp_path / "outputs"
    outputs.mkdir()
    _write_gzip_jsonl(outputs / "chunk_00000.out.jsonl.gz", [{
        "document_id": "parent", "parse_status": "json_parse_error"
    }])
    repair = tmp_path / "repair"
    assert prepare_quote_normalized(export_dir, outputs, repair) == 1
    row = read_rows(repair / "chunks" / "chunk_00000.jsonl.gz")[0]
    assert row["repair_parent_document_id"] == "parent"
    assert row["repair_source_normalization"] == "quotation_marks_to_spaces"
    assert all(char not in row["source_text"] for char in '\"“”‘’')
    original = 'The Wyden-authored\n“Secure Rural Schools Act"\n– brought funding.'
    normalized = 'The Wyden-authored Secure Rural Schools Act – brought funding.'
    assert reanchor_normalized_evidence(normalized, original) == original
