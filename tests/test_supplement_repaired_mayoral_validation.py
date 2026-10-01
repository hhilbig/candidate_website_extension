import csv
import gzip
import hashlib
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
from supplement_repaired_mayoral_validation import read_capture, supplement, variant_key


def write_csv(path, rows):
    with path.open("w") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def test_variants_preserve_path_query():
    assert variant_key("https://a.com") == variant_key("https://a.com/")
    assert variant_key("https://a.com?q=1") == variant_key("https://a.com/?q=1")
    assert variant_key("http://www.a.com:80/x?q=1") == variant_key("https://a.com/x?q=1")
    assert variant_key("https://a.com/x?q=1") != variant_key("https://a.com/x?q=2")
    assert variant_key("https://a.com/x") != variant_key("https://a.com/x/")
    assert variant_key("http://a.com:443/x") != variant_key("https://a.com/x")


def test_bounded_hash_verified_read(tmp_path):
    raw = b"hello"
    (tmp_path / "a.gz").write_bytes(gzip.compress(raw))
    cap = dict(raw_html_path="a.gz", raw_html_sha256=hashlib.sha256(raw).hexdigest())
    assert read_capture(tmp_path, cap) == raw
    with pytest.raises(ValueError, match="byte limit"):
        read_capture(tmp_path, cap, 4)
    with pytest.raises(ValueError, match="hash mismatch"):
        read_capture(tmp_path, {**cap, "raw_html_sha256": "bad"})
    with pytest.raises(ValueError, match="escapes"):
        read_capture(tmp_path, {**cap, "raw_html_path": "../other.gz"})


def test_selection_and_all_input_keys(tmp_path):
    inp = tmp_path / "input.csv"
    rows = [dict(candidate_url_id="one", full_name="Ann Smith", geo_name="Springfield", year="2020", month="11.0", exact_normalized_url="https://a.com/")]
    rows.append({**rows[0], "candidate_url_id": "two", "exact_normalized_url": "https://b.com/"})
    write_csv(inp, rows)
    raw = b"<h1>Ann Smith for Mayor Springfield</h1>"
    (tmp_path / "a.gz").write_bytes(gzip.compress(raw))
    cap = dict(capture_id="a", original_url="http://www.a.com/", effective_timestamp="20201115000000", timestamp="20201115000000", fetch_status="fetched", raw_html_path="a.gz", raw_html_sha256=hashlib.sha256(raw).hexdigest())
    write_csv(tmp_path / "fetch_attempts.csv", [cap, {**cap, "capture_id": "b", "original_url": "https://a.com/", "effective_timestamp": "20201116000000"}, {**cap, "capture_id": "c", "original_url": "https://b.com/", "effective_timestamp": "20220101000000"}])
    result, summary = supplement(inp, tmp_path, tmp_path / "out")
    assert summary["output_rows"] == 2
    assert result[0]["capture_id"] == "b"
    assert result[0]["saved_capture_status"] == "saved_capture_content_confirmed_reuse_pending"
    assert result[0]["strict_coverage_eligible_result"] is False
    assert result[1]["saved_capture_status"] == "not_found_in_saved_corpus"
