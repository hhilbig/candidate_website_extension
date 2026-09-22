"""Tests for the round-four endorsement release adapter."""
from __future__ import annotations

import copy

import pytest

from src.candidate_extraction import build_endorsement_context_provenance
from src.candidate_endorsement_round4_production import (
    ROUND4_DEVELOPMENT_ARTIFACT_SCOPE,
    ROUND4_DEVELOPMENT_AUTHORIZATIONS,
    ROUND4_PRODUCTION_ARTIFACT_SCOPE,
    ROUND4_PRODUCTION_AUTHORIZATIONS,
)
from src.candidate_endorsement_round4_release import (
    ENDORSEMENT_ARCHITECTURE,
    RECORD_LEDGER_VERSION,
    Round4ProjectionError,
    apply_endorsement_round4_ledger,
    build_round4_record_ledger,
    classify_stage_b_response,
    project_round4_stage_b_batch,
    round4_parent_coverage,
    validate_endorsement_round4_record_ledger_private_rows,
)

LINEAGE_FIELDS = (
    "line_inventory_id",
    "source_line_id",
    "stage_a_batch_id",
    "stage_a_slot",
    "stage_b_batch_id",
    "stage_b_slot",
    "model_record_index",
    "carrier_alignment_source_model_item_id",
    "inventory_owner_segment_id",
    "record_evidence_carrier_segment_id",
    "target_block_alignment_method",
    "target_block_exact_match_count",
    "record_evidence_parent_start_char",
    "record_evidence_parent_end_char",
)

PARENT_TEXT = (
    "Endorsements\n"
    "Democratic Congressional Campaign Committee\n"
    "Ann Kirkpatrick\n"
)
HEADING_END = len("Endorsements")
FIRST_START = len("Endorsements\n")
FIRST_END = FIRST_START + len("Democratic Congressional Campaign Committee")
SECOND_START = FIRST_END + 1
SECOND_END = SECOND_START + len("Ann Kirkpatrick")


def _context_provenance(segment_id):
    """Build genuine context provenance with the frozen production builder."""
    return build_endorsement_context_provenance(
        parent_document_id="parent-1",
        parent_source_text=PARENT_TEXT,
        source_urls=[
            "https://web.archive.org/web/20200101000000/"
            "http://example.org/endorsements/"
        ],
        segment_id=segment_id,
        segment_start_char=0,
        segment_end_char=len(PARENT_TEXT),
    )


def _line(index, start, end, source_line_id, owner):
    return {
        "source_line_id": source_line_id,
        "parent_line_index": index,
        "parent_start_char": start,
        "parent_end_char": end,
        "parent_full_end_char": end + 1,
        "line_text": PARENT_TEXT[start:end],
        "line_text_sha256": f"hash-{source_line_id}",
        "line_terminator": "\n",
        "model_decision_required": True,
        "deterministic_disposition": "model_decision_required",
        "owner_segment_id": owner,
    }


def _parent():
    segment_id = "segment-a"
    return {
        "parent_document_id": "parent-1",
        "extraction_source_text_sha256": "source-hash",
        "parent_source_text": PARENT_TEXT,
        "parent_line_inventory_sha256": "inventory-hash",
        "segment_descriptors": [
            {
                "segment_id": segment_id,
                "segment_index": 0,
                "segment_start_char": 0,
                "segment_end_char": len(PARENT_TEXT),
                "source_text": PARENT_TEXT,
                "endorsement_context_provenance": _context_provenance(
                    segment_id
                ),
            }
        ],
        "lines": [
            _line(0, 0, HEADING_END, "line-0", segment_id),
            _line(1, FIRST_START, FIRST_END, "line-1", segment_id),
            _line(2, SECOND_START, SECOND_END, "line-2", segment_id),
        ],
    }


def _target(slot, source_line_id, start, end, stage_a_slot):
    line = {
        "source_line_id": source_line_id,
        "parent_line_index": 1,
        "parent_start_char": start,
        "parent_end_char": end,
        "parent_full_end_char": end + 1,
        "line_text": PARENT_TEXT[start:end],
    }
    return {
        "slot": slot,
        "source_line_id": source_line_id,
        "stage_a_batch_id": "stage-a-batch",
        "stage_a_slot": stage_a_slot,
        "stage_a_role": "record_anchor",
        "line_text": PARENT_TEXT[start:end],
        "record_block_lines": [line],
        "record_block_text": PARENT_TEXT[start:end + 1],
    }


def _batch():
    return {
        "stage": "record_extraction",
        "batch_id": "stage-b-batch",
        "parent_document_id": "parent-1",
        "extraction_source_text_sha256": "source-hash",
        "targets": [
            _target("slot_00", "line-1", FIRST_START, FIRST_END, "slot_01"),
            _target("slot_01", "line-2", SECOND_START, SECOND_END, "slot_02"),
        ],
    }


def _record(name_parts, category="other_organization", entity="organization"):
    return {
        "name_parts": list(name_parts),
        "entity_type": entity,
        "endorser_category": category,
        "title": None,
        "affiliation": None,
        "jurisdiction": None,
    }


def _output(slot_records, *, parse_status="parsed", technical_pass=True):
    return {
        "stage": "record_extraction",
        "batch_id": "stage-b-batch",
        "parent_document_id": "parent-1",
        "parse_status": parse_status,
        "finish_reason": "stop",
        "technical_pass": technical_pass,
        "input_truncated": False,
        "authorizations": dict(ROUND4_DEVELOPMENT_AUTHORIZATIONS),
        "model_output": slot_records,
        "stage_validation": {
            "schema_valid": True, "errors": [], "source_projection": {},
        },
    }


def _records_decision(records):
    return {
        "disposition": "records",
        "source_mentions_fully_accounted": True,
        "records": records,
    }


def _no_record_decision(disposition="not_endorsement"):
    return {
        "disposition": disposition,
        "source_mentions_fully_accounted": disposition != "unclear",
        "records": [],
    }


def _project(batch=None, output=None, parent=None):
    return project_round4_stage_b_batch(
        batch or _batch(),
        output or _output({
            "slot_00": _records_decision([
                _record(["Democratic Congressional Campaign Committee"])
            ]),
            "slot_01": _records_decision([
                _record(["Ann Kirkpatrick"], "government_official", "individual")
            ]),
        }),
        parent or _parent(),
        artifact_scope=ROUND4_DEVELOPMENT_ARTIFACT_SCOPE,
    )


def test_every_lineage_field_is_present_and_exact():
    result = _project()
    assert result["aligned_record_count"] == 2
    assert result["withheld_record_count"] == 0
    for record in result["aligned_records"]:
        for field in LINEAGE_FIELDS:
            assert field in record, field
        assert record["line_inventory_id"] == "inventory-hash"
        assert record["stage_b_batch_id"] == "stage-b-batch"
        assert record["target_block_exact_match_count"] == 1
        start = record["record_evidence_parent_start_char"]
        end = record["record_evidence_parent_end_char"]
        assert PARENT_TEXT[start:end] == record["record_evidence"]


def test_lineage_keeps_the_stage_a_slot_of_each_target():
    records = {
        record["stage_b_slot"]: record for record in _project()["aligned_records"]
    }
    assert records["slot_00"]["stage_a_slot"] == "slot_01"
    assert records["slot_01"]["stage_a_slot"] == "slot_02"
    assert records["slot_00"]["source_line_id"] == "line-1"
    assert records["slot_01"]["source_line_id"] == "line-2"


def test_record_identity_differs_from_the_carrier_alignment_identity():
    record = _project()["aligned_records"][0]
    assert (
        record["source_model_item_id"]
        != record["carrier_alignment_source_model_item_id"]
    )


def test_a_name_without_an_ordered_projection_is_unresolved():
    """This is the failure the 11 known independent-sample blocks hit."""
    output = _output({
        "slot_00": _records_decision([_record(["Absent Endorser Name"])]),
        "slot_01": _no_record_decision(),
    })
    result = _project(output=output)
    assert result["aligned_record_count"] == 0
    assert result["withheld_record_count"] == 1
    entry = result["unresolved_target_blocks"][0]
    assert entry["reason"] == "name_parts_have_no_ordered_source_projection"
    assert entry["source_line_id"] == "line-1"


def test_an_unresolved_block_never_becomes_a_measured_zero():
    output = _output({
        "slot_00": _records_decision([_record(["Absent Endorser Name"])]),
        "slot_01": _no_record_decision(),
    })
    result = _project(output=output)
    summary = round4_parent_coverage("parent-1", [], [result])
    assert summary["aligned_record_count"] == 0
    assert summary["withheld_record_count"] == 1
    assert summary["processing_status"] == "processed_all_withheld"
    assert summary["public_coverage_complete"] is False


def test_an_optional_field_outside_the_block_is_nulled_and_recorded():
    record = _record(["Ann Kirkpatrick"], "government_official", "individual")
    record["title"] = "Senator"
    output = _output({
        "slot_00": _no_record_decision(),
        "slot_01": _records_decision([record]),
    })
    result = _project(output=output)
    assert result["aligned_record_count"] == 1
    assert result["aligned_records"][0]["title"] is None
    repair = result["optional_field_repairs"][0]
    assert repair["field"] == "title"
    assert repair["original_value"] == "Senator"
    assert repair["replacement_value"] is None


def test_a_not_endorsement_decision_keeps_the_parent_complete():
    output = _output({
        "slot_00": _no_record_decision(),
        "slot_01": _no_record_decision(),
    })
    result = _project(output=output)
    assert result["aligned_record_count"] == 0
    assert result["withheld_record_count"] == 0
    summary = round4_parent_coverage("parent-1", [], [result])
    assert summary["public_coverage_complete"] is True
    assert summary["processing_status"] == "processed_success"


def test_a_stage_a_unclear_line_makes_the_parent_incomplete():
    """Round four has no adjudication stage that can clear an unclear line."""
    result = _project()
    stage_a_rows = [{
        "batch_id": "stage-a-batch",
        "model_output": {"slot_00": {"role": "unclear"}},
    }]
    summary = round4_parent_coverage("parent-1", stage_a_rows, [result])
    assert summary["stage_a_unclear_lines"] == ["stage-a-batch:slot_00"]
    assert summary["public_coverage_complete"] is False
    # The records still publish.  Only the coverage claim changes.
    assert summary["aligned_record_count"] == 2
    assert summary["processing_status"] == "processed_success"


@pytest.mark.parametrize(
    "row,expected",
    [
        ({"technical_pass": True, "parse_status": "parsed",
          "finish_reason": "stop"}, "complete"),
        ({"technical_pass": False, "parse_status": "invalid_stage_output",
          "finish_reason": "stop",
          "stage_validation": {"errors": ["slot_00 title: not exact"]}},
         "optional_field_repaired"),
        ({"technical_pass": False, "parse_status": "invalid_stage_output",
          "finish_reason": "stop",
          "stage_validation": {"errors": ["slot_00 name_parts: no projection"]}},
         "unresolved_target_block"),
        ({"technical_pass": False, "parse_status": "strict_json_parse_error",
          "finish_reason": "stop"}, "infrastructure_failure"),
        ({"technical_pass": False, "parse_status": "parsed",
          "finish_reason": "length"}, "infrastructure_failure"),
        ({"technical_pass": False, "parse_status": "invalid_stage_output",
          "finish_reason": "stop",
          "stage_validation": {"schema_valid": False, "errors": ["bad batch"]}},
         "unlocalizable_schema_failure"),
    ],
)
def test_failure_classification(row, expected):
    assert classify_stage_b_response(row)[0] == expected


def test_a_retryable_failure_blocks_the_projection_and_the_status():
    output = _output({"slot_00": {}, "slot_01": {}},
                     parse_status="strict_json_parse_error",
                     technical_pass=False)
    result = _project(output=output)
    assert result["retry_required"] is True
    assert result["aligned_records"] == []
    assert len(result["unresolved_target_blocks"]) == 2
    summary = round4_parent_coverage("parent-1", [], [result])
    assert summary["retry_required"] is True
    with pytest.raises(Round4ProjectionError, match="retryable failure"):
        build_round4_record_ledger(
            [summary],
            stage_a_input_manifest_sha256="a",
            stage_a_generation_config_sha256="b",
            stage_b_input_manifest_sha256="c",
            stage_b_generation_config_sha256="d",
            stage_b_response_count=1,
            accounted_output_count=1,
        )


def test_a_mismatched_batch_and_output_identity_is_rejected():
    output = _output({"slot_00": _no_record_decision(),
                      "slot_01": _no_record_decision()})
    output["batch_id"] = "other-batch"
    with pytest.raises(Round4ProjectionError, match="identities differ"):
        _project(output=output)


def test_a_production_scope_rejects_development_authorizations():
    batch, parent = _batch(), _parent()
    output = _output({"slot_00": _no_record_decision(),
                      "slot_01": _no_record_decision()})
    with pytest.raises(ValueError, match="do not match the artifact scope"):
        project_round4_stage_b_batch(
            batch, output, parent,
            artifact_scope=ROUND4_PRODUCTION_ARTIFACT_SCOPE,
        )


def test_a_target_that_is_not_a_stage_a_anchor_is_rejected():
    batch = _batch()
    batch["targets"][0]["stage_a_role"] = "nonrecord_content"
    with pytest.raises(Round4ProjectionError, match="not a Stage-A anchor"):
        _project(batch=batch)


def test_a_source_hash_mismatch_is_rejected():
    parent = _parent()
    parent["extraction_source_text_sha256"] = "different"
    with pytest.raises(Round4ProjectionError, match="source identities differ"):
        _project(parent=parent)


def _ledger():
    summary = round4_parent_coverage("parent-1", [], [_project()])
    return build_round4_record_ledger(
        [summary],
        stage_a_input_manifest_sha256="stage-a-manifest",
        stage_a_generation_config_sha256="stage-a-config",
        stage_b_input_manifest_sha256="stage-b-manifest",
        stage_b_generation_config_sha256="stage-b-config",
        stage_b_response_count=1,
        accounted_output_count=1,
    )


def test_a_consistent_ledger_validates():
    ledger = _ledger()
    assert ledger["summary"]["version"] == RECORD_LEDGER_VERSION
    assert ledger["summary"]["all_model_records_accounted"] is True
    assert ledger["summary"]["unaccounted_outputs"] == 0
    recomputed = validate_endorsement_round4_record_ledger_private_rows(ledger)
    assert recomputed["aligned_record_count"] == 2
    assert recomputed["parent_count"] == 1


def test_a_tampered_summary_total_is_rejected():
    ledger = _ledger()
    ledger["summary"]["aligned_record_count"] = 99
    with pytest.raises(
        Round4ProjectionError, match="differs from the recomputed value"
    ):
        validate_endorsement_round4_record_ledger_private_rows(ledger)


def test_a_tampered_parent_summary_is_rejected():
    ledger = _ledger()
    ledger["parents"][0]["public_coverage_complete"] = False
    with pytest.raises(Round4ProjectionError):
        validate_endorsement_round4_record_ledger_private_rows(ledger)


def test_an_unaccounted_output_is_rejected():
    summary = round4_parent_coverage("parent-1", [], [_project()])
    ledger = build_round4_record_ledger(
        [summary],
        stage_a_input_manifest_sha256="a",
        stage_a_generation_config_sha256="b",
        stage_b_input_manifest_sha256="c",
        stage_b_generation_config_sha256="d",
        stage_b_response_count=5,
        accounted_output_count=1,
    )
    assert ledger["summary"]["unaccounted_outputs"] == 4
    with pytest.raises(
        Round4ProjectionError, match="does not account for every model output"
    ):
        validate_endorsement_round4_record_ledger_private_rows(ledger)


def test_a_contract_hash_mismatch_is_rejected():
    ledger = _ledger()
    ledger["summary"]["round4_production_contract_sha256"] = "0" * 64
    with pytest.raises(Round4ProjectionError, match="contract hash differs"):
        validate_endorsement_round4_record_ledger_private_rows(ledger)


def test_an_untyped_unresolved_block_is_rejected():
    output = _output({
        "slot_00": _records_decision([_record(["Absent Endorser Name"])]),
        "slot_01": _no_record_decision(),
    })
    summary = round4_parent_coverage("parent-1", [], [_project(output=output)])
    ledger = build_round4_record_ledger(
        [summary],
        stage_a_input_manifest_sha256="a",
        stage_a_generation_config_sha256="b",
        stage_b_input_manifest_sha256="c",
        stage_b_generation_config_sha256="d",
        stage_b_response_count=1,
        accounted_output_count=1,
    )
    ledger["parents"][0]["unresolved_target_blocks"][0]["reason"] = ""
    with pytest.raises(Round4ProjectionError, match="no typed reason"):
        validate_endorsement_round4_record_ledger_private_rows(ledger)


def test_the_overlay_preserves_the_superseded_status_and_empties_variants():
    summary = round4_parent_coverage("parent-1", [], [_project()])
    row = {
        "route": "endorsements",
        "document_id": "segment-a",
        "parent_document_id": "parent-1",
        "processing_status": "processed_success",
        "record_variants": [{"variant": "one-stage"}],
        "resolved_records": [{"record": "one-stage"}],
    }
    updated = apply_endorsement_round4_ledger(row, summary, [{"record": "new"}])
    assert updated["superseded_one_stage_processing_status"] == (
        "processed_success"
    )
    assert updated["record_variants"] == []
    assert updated["resolved_records"] == [{"record": "new"}]
    assert updated["extraction_architecture"] == ENDORSEMENT_ARCHITECTURE
    assert updated["endorsement_round4_record_ledger"][
        "public_coverage_complete"
    ] is True
    # The original row is never mutated.
    assert row["record_variants"] == [{"variant": "one-stage"}]


def test_the_overlay_refuses_a_row_from_another_route():
    summary = round4_parent_coverage("parent-1", [], [_project()])
    with pytest.raises(Round4ProjectionError, match="only to an endorsement"):
        apply_endorsement_round4_ledger(
            {"route": "issues"}, summary, []
        )


def test_projection_is_deterministic():
    first = _project()
    second = _project()
    assert first["aligned_records"] == second["aligned_records"]


def test_a_record_beyond_the_guided_maximum_is_rejected():
    records = [_record(["Ann Kirkpatrick"], "government_official",
                       "individual")] * 33
    output = _output({
        "slot_00": _no_record_decision(),
        "slot_01": _records_decision(records),
    })
    result = _project(output=output)
    entry = result["unresolved_target_blocks"][0]
    assert entry["reason"] == "invalid_slot_decision"
    assert "guided maximum" in entry["detail"]


def test_an_unknown_disposition_is_rejected():
    output = _output({
        "slot_00": {"disposition": "made_up", "records": [],
                    "source_mentions_fully_accounted": True},
        "slot_01": _no_record_decision(),
    })
    result = _project(output=output)
    assert result["unresolved_target_blocks"][0]["reason"] == (
        "invalid_slot_decision"
    )


def test_a_missing_slot_decision_is_unresolved_not_dropped():
    output = _output({"slot_01": _no_record_decision()})
    result = _project(output=output)
    entry = result["unresolved_target_blocks"][0]
    assert entry["reason"] == "missing_slot_decision"
    assert entry["stage_b_slot"] == "slot_00"


def test_an_entity_type_outside_its_category_is_unresolved():
    bad = _record(["Ann Kirkpatrick"], "labor_union", "individual")
    output = _output({
        "slot_00": _no_record_decision(),
        "slot_01": _records_decision([bad]),
    })
    result = _project(output=output)
    assert result["withheld_record_count"] == 1
    assert result["unresolved_target_blocks"][0]["reason"] == (
        "invalid_model_record"
    )
