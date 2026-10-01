import csv
import gzip
import importlib.util
import json
from pathlib import Path

import pytest

SPEC = importlib.util.spec_from_file_location("finalizer", Path(__file__).parents[1] / "scripts/finalize_repaired_mayoral_validation.py")
m = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(m)


@pytest.fixture
def batch(tmp_path):
    source = tmp_path / "input.csv"
    raw = tmp_path / "raw"
    (raw / "evidence").mkdir(parents=True)
    rows = [{"candidate_url_id": str(i), "candidate_year_id": str(i) + "__2020",
             "candidate_contest_id": "contest", "full_name": "Jane Candidate", "geo_name": "City",
             "state_abb": "CA", "year": "2020", "month": "11.0", "exact_normalized_url": "http://candidate.com/",
             "source_expansion_claim_ids_json": '["claim"]', "human_accepted": "False"} for i in range(120)]
    with source.open("w") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0])
        writer.writeheader()
        writer.writerows(rows)
    digest = m.sha256(source.read_bytes())
    (raw / "manifest.json").write_text(json.dumps({"input_sha256": digest, "input_rows": 120}))
    body = b"<html>Jane Candidate for mayor</html>"
    sha = m.sha256(body)
    (raw / "evidence" / (sha + ".gz")).write_bytes(gzip.compress(body))
    (raw / "evidence/requests.jsonl").write_text(json.dumps({"sha256": sha, "evidence_file": sha + ".gz", "bytes": len(body)}) + "\n")
    results = [{**r, "validation_key": r["candidate_url_id"], "input_sha256": digest,
                "source_claim_ids_json": r["source_expansion_claim_ids_json"],
                "temporal_validation_status": "grade_a_content_confirmed_pending_review",
                "validation_reason": "confirmed", "proposed_evidence_grade": "A",
                "strict_coverage_eligible_result": True, "capture_content_sha256": sha, "month": 11,
                "within_headline_window": True,
                "capture_effective_url": "https://web.archive.org/web/20201115120000id_/http://candidate.com/"} for r in rows]
    def save():
        (raw / "results.jsonl").write_text("".join(json.dumps(r) + "\n" for r in results))
    save()
    return source, raw, tmp_path / "final", results, save, sha


def test_finalize_and_preserve_raw(batch):
    source, raw, output, results, save, sha = batch
    before = (raw / "results.jsonl").read_bytes()
    results.append(dict(results[0], validation_reason="latest"))
    save()
    before = (raw / "results.jsonl").read_bytes()
    summary = m.finalize(source, raw, output)
    assert summary["observed_keys"] == 120
    assert summary["superseded_results"] == 1
    assert summary["strict_eligible"] == 120
    assert (raw / "results.jsonl").read_bytes() == before
    assert list(csv.DictReader((output / "validation.csv").open()))[0]["validation_reason"] == "latest"


@pytest.mark.parametrize("kind", ["unknown", "missing", "hash", "context", "month", "claims", "evidence", "request"])
def test_reject_invalid_provenance(batch, kind):
    source, raw, output, results, save, sha = batch
    if kind == "unknown": results[0]["validation_key"] = "unknown"
    if kind == "missing": results.pop()
    if kind == "hash": results[0]["input_sha256"] = "bad"
    if kind == "context": results[0]["year"] = "2024"
    if kind == "month": results[0]["month"] = 10
    if kind == "claims": results[0]["source_claim_ids_json"] = "[]"
    if kind == "evidence": (raw / "evidence" / (sha + ".gz")).write_bytes(gzip.compress(b"bad"))
    if kind == "request": (raw / "evidence/requests.jsonl").write_text('{}\n')
    save()
    with pytest.raises(ValueError): m.finalize(source, raw, output)


def test_reviews_only_downgrade(batch):
    source, raw, output, results, save, sha = batch
    results[1].update(temporal_validation_status="request_error_retryable", strict_coverage_eligible_result=False, proposed_evidence_grade="C")
    save()
    reviews = {str(i): {"expected_capture_sha256": sha, "historical_campaign_content_supported": bool(i),
                       "review_reason": "Checked archived text", "reviewer": "codex_archived_text"} for i in range(2)}
    summary = m.finalize(source, raw, output, reviews)
    assert summary["strict_eligible"] == 118
    rows = list(csv.DictReader((output / "validation.csv").open()))
    assert rows[0]["final_validation_status"] == "content_validation_unresolved"
    assert rows[1]["final_validation_status"] == "request_error_retryable"
    assert rows[0]["human_accepted"] == "False"
    reviews["0"]["expected_capture_sha256"] = "wrong"
    with pytest.raises(ValueError): m.finalize(source, raw, output, reviews)


@pytest.mark.parametrize("url, expected", [("https://www.candidate.com/", "scheme_www_defaultport_equivalent"),
    ("http://candidate.com", "scheme_www_defaultport_equivalent"),
    ("http://candidate.com:80/", "scheme_www_defaultport_equivalent"),
    ("http://candidate.com/news", "changed_target"), ("http://candidate.com/?q=1", "changed_target"),
    ("http://other.com/", "changed_target"), ("http://candidate.com/", "literal")])
def test_equivalence(url, expected):
    assert m.url_equivalence("http://candidate.com/", url) == expected


def test_changed_replay_target_downgrades(batch):
    source, raw, output, results, save, sha = batch
    results[0]["capture_effective_url"] = "https://wayback.archive-it.org/all/20201115120000id_/http://candidate.com/news?q=1"
    save()
    assert m.finalize(source, raw, output)["strict_eligible"] == 119
    assert m.replay_target(results[0]["capture_effective_url"]) == "http://candidate.com/news?q=1"


def test_saved_corpus_repairs_negative_without_approval(batch):
    source, raw, output, results, save, sha = batch
    results[0].update(temporal_validation_status="no_archive_capture", capture_content_sha256=None,
                      capture_effective_url=None, strict_coverage_eligible_result=False)
    save()
    supplement = output.parent / "saved.csv"
    saved = [{"candidate_url_id": str(i), "input_sha256": m.sha256(source.read_bytes()),
              "saved_capture_status": "saved_capture_content_confirmed_reuse_pending",
              "hash_verified": "True", "raw_html_sha256": sha, "days_absolute": "120",
              "raw_html_path": sha + ".gz", "corpus_dir": str(raw / "evidence"),
              "effective_replay_url": "https://web.archive.org/web/20201115120000id_/http://candidate.com/"} for i in range(120)]
    with supplement.open("w") as handle:
        writer = csv.DictWriter(handle, fieldnames=saved[0])
        writer.writeheader()
        writer.writerows(saved)
    summary = m.finalize(source, raw, output, saved_supplement=supplement)
    assert summary["statuses"]["saved_capture_content_confirmed_reuse_pending"] == 1
    assert summary["strict_eligible"] == 119
    final = list(csv.DictReader((output / "validation.csv").open()))
    assert final[0]["automated_validation_status"] == "no_archive_capture"
    assert final[0]["capture_content_sha256"] == ""
    assert final[1]["capture_content_sha256"] == sha
    assert final[0]["human_accepted"] == "False"
    saved[0]["saved_capture_status"] = "saved_capture_content_unresolved_reuse_pending"
    with supplement.open("w") as handle:
        writer = csv.DictWriter(handle, fieldnames=saved[0])
        writer.writeheader()
        writer.writerows(saved)
    reviews = {"0": {"expected_capture_sha256": sha, "historical_campaign_content_supported": True,
                     "review_reason": "City alias confirmed in archived campaign text", "reviewer": "codex_archived_text"}}
    summary = m.finalize(source, raw, output, reviews=reviews, saved_supplement=supplement)
    assert summary["statuses"]["saved_capture_content_confirmed_reuse_pending"] == 1
    final = list(csv.DictReader((output / "validation.csv").open()))
    assert final[0]["strict_coverage_eligible_result"] == "False"
    assert final[0]["capture_content_sha256"] == ""
    assert final[0]["automated_validation_status"] == "no_archive_capture"
    reviews["0"]["expected_capture_sha256"] = "wrong"
    with pytest.raises(ValueError, match="Review capture hash"):
        m.finalize(source, raw, output, reviews=reviews, saved_supplement=supplement)


def test_reject_unknown_review_and_raw_output(batch):
    source, raw, output, *_ = batch
    with pytest.raises(ValueError, match="Unknown review"):
        m.finalize(source, raw, output, {"unknown": {}})
    with pytest.raises(ValueError, match="separate"):
        m.finalize(source, raw, raw)


def test_positive_review_requires_headline_window(batch):
    source, raw, output, results, save, sha = batch
    results[0]["within_headline_window"] = False
    save()
    review = {"0": {"expected_capture_sha256": sha, "historical_campaign_content_supported": True,
                    "review_reason": "Campaign text", "reviewer": "codex_archived_text"}}
    with pytest.raises(ValueError, match="election-period"):
        m.finalize(source, raw, output, review)


def test_missing_month_normalization():
    assert m.normalized_month("") is None
    assert m.normalized_month(None) is None
    assert m.normalized_month("11.0") == 11
    with pytest.raises(ValueError): m.normalized_month("11.5")
