import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
from resolve_mayoral_validation_flags import resolution, side_assessment, read_html
import gzip
import hashlib
import pytest


def test_only_complete_same_candidate_checks_clear():
    assert resolution(True, ["same_candidate"]) == "text_rule_flags_cleared"
    assert resolution(True, []) == "surrounding_content_insufficient"
    assert resolution(False, ["same_candidate"]) == "surrounding_content_insufficient"
    assert resolution(True, ["same_candidate"], True) == "saved_sides_consistent_inventory_incomplete"
    assert resolution(True, [], True) == "saved_side_evidence_missing_or_insufficient"


def test_errors_and_genuine_reuse_are_separate():
    assert resolution(True, ["archive_service_error"]) == "archive_service_error_requires_replay"
    assert resolution(True, ["unrelated_content_confirmed"]) == "later_domain_reuse_confirmed"
    assert side_assessment("<h1>Rate limit reached</h1>Aitratelimit@archive.org", "Jane Doe", "Boston", "a", {}) == "archive_service_error"
    assert side_assessment("", "Jane Doe", "Boston", "a", {}) == "insufficient_surrounding_content"
    assert side_assessment("Jane Doe", "Jane Doe", "Boston", "a", {}) == "same_candidate"


def test_content_hash_required(tmp_path):
    p = tmp_path / "body.gz"
    p.write_bytes(gzip.compress(b"hello"))
    assert read_html(p, hashlib.sha256(b"hello").hexdigest()) == "hello"
    with pytest.raises(ValueError, match="hash"):
        read_html(p, "invalid")
