"""The one-stage output audit must catch the two defects that stopped prior runs.

Both frozen failed runs are kept in the repository as immutable technical
evidence. They make the best possible regression fixtures: if this audit cannot
rediscover the defect that cancelled each run, it cannot be trusted to catch a
recurrence.
"""
from __future__ import annotations

import gzip
import json
from pathlib import Path

import pytest

from scripts.audit_candidate_extraction_one_stage_output import (
    audit, repeated_record_counts,
)

REPO = Path(__file__).resolve().parents[1]
BUILD = REPO / "build/hive_candidate_extraction"
CONTROL_RUN = BUILD / "v2_1_full_hive_output_20260824e_failed"
WHITESPACE_RUN = BUILD / "v2_1_full_hive_output_20260824k_failed"


@pytest.mark.skipif(not CONTROL_RUN.is_dir(), reason="frozen failed run absent")
def test_it_rediscovers_the_control_character_failure():
    """Job 21250200 wrote literal control characters into JSON strings."""
    result = audit(CONTROL_RUN)
    assert result["clean"] is False
    assert result["parse_status_counts"]["strict_json_parse_error"] == 6
    assert result["defect_counts"]["parse_status_not_parsed"] == 6
    # The frozen record states every failing row was an endorsement row.
    assert {row["route"] for row in result["defects"]["parse_status_not_parsed"]} == {
        "endorsements"
    }


@pytest.mark.skipif(not WHITESPACE_RUN.is_dir(), reason="frozen failed run absent")
def test_it_rediscovers_the_whitespace_truncation_failure():
    """Job 21296223 emitted grammar-permitted tabs until it hit the output cap."""
    result = audit(WHITESPACE_RUN)
    assert result["clean"] is False
    assert result["defect_counts"]["output_truncated_finish_reason_length"] == 1
    assert result["finish_reason_counts"]["length"] == 1
    assert result["parse_status_counts"]["output_truncated"] == 1
    # The row consumed its entire output budget, which is the signature.
    assert result["highest_output_token_use_fraction_of_cap"] == 1.0
    failing = result["defects"]["output_truncated_finish_reason_length"][0]
    assert failing["document_id"].startswith("f151afdf")
    assert failing["route"] == "biography"


def test_a_clean_run_reports_clean(tmp_path: Path):
    rows = [{
        "document_id": "d1", "route": "issues", "parse_status": "parsed",
        "finish_reason": "stop", "output_tokens": 500,
        "max_output_tokens": 24576,
        "model_extraction": {"issue_blocks": [{"claim": "a"}, {"claim": "b"}]},
    }]
    payload = "".join(json.dumps(r) + "\n" for r in rows).encode("utf-8")
    with gzip.open(tmp_path / "chunk_00000.out.jsonl.gz", "wb") as handle:
        handle.write(payload)
    result = audit(tmp_path)
    assert result["clean"] is True
    assert result["rows"] == 1
    assert all(v == 0 for v in result["defect_counts"].values())


def test_exact_item_repetition_is_detected():
    """A prior run emitted 299 identical copies of one record."""
    same = {"claim": "identical"}
    assert repeated_record_counts({"issue_blocks": [same, same, same]}) == 3
    assert repeated_record_counts({"issue_blocks": [{"a": 1}, {"b": 2}]}) == 1
    assert repeated_record_counts(None) == 0


def test_canonicalized_controls_are_reported_but_not_a_defect(tmp_path: Path):
    """The runner repairs control characters by design; only unrepaired ones fail."""
    row = {
        "document_id": "d1", "route": "biography", "parse_status": "parsed",
        "finish_reason": "stop", "output_tokens": 10, "max_output_tokens": 24576,
        "json_control_canonicalization_count": 4,
        "json_control_unrepaired_after_escape_count": 0,
        "model_extraction": {"facts": []},
    }
    with gzip.open(tmp_path / "chunk_00000.out.jsonl.gz", "wb") as handle:
        handle.write((json.dumps(row) + "\n").encode("utf-8"))
    result = audit(tmp_path)
    assert result["rows_with_canonicalized_controls"] == 1
    assert result["clean"] is True


def test_unrepaired_controls_are_a_defect(tmp_path: Path):
    row = {
        "document_id": "d1", "route": "biography", "parse_status": "parsed",
        "finish_reason": "stop", "output_tokens": 10, "max_output_tokens": 24576,
        "json_control_unrepaired_after_escape_count": 2,
        "model_extraction": {"facts": []},
    }
    with gzip.open(tmp_path / "chunk_00000.out.jsonl.gz", "wb") as handle:
        handle.write((json.dumps(row) + "\n").encode("utf-8"))
    result = audit(tmp_path)
    assert result["clean"] is False
    assert result["defect_counts"]["control_characters_unrepaired_in_strings"] == 1
