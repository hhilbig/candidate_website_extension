import csv
import importlib.util
import sys
from pathlib import Path

import pytest
import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
import validate_repaired_mayoral_leads as module


def test_missing_validation_values_are_json_serializable():
    import json
    result = module.serializable_result({"distance": module.pd.NA, "retryable": False})
    assert json.loads(json.dumps(result)) == {"distance": None, "retryable": False}


def test_prepare_keys_and_month(tmp_path):
    path = tmp_path / "input.csv"
    path.write_text("candidate_url_id,month\na,11.0\nb,\n")
    rows = module.prepare_rows(path)
    assert rows[0]["month"] == 11
    assert rows[1]["month"] == ""
    path.write_text("candidate_url_id,month\na,11\na,12\n")
    with pytest.raises(ValueError, match="unique"):
        module.prepare_rows(path)


def test_live_escape_blocked(tmp_path):
    session = module.EvidenceSession(tmp_path)
    with pytest.raises(requests.RequestException, match="escaped"):
        session.get("https://candidate.com/", headers={}, timeout=1)
    assert "escaped" in (tmp_path / "requests.jsonl").read_text()


def test_response_bound(tmp_path, monkeypatch):
    class Response:
        is_redirect = False
        def iter_content(self, _):
            yield b"x" * 11
        def close(self):
            pass
    session = module.EvidenceSession(tmp_path)
    monkeypatch.setattr(module, "MAX_BODY", 10)
    monkeypatch.setattr(session.session, "get", lambda *a, **k: Response())
    with pytest.raises(requests.RequestException, match="limit"):
        session.get("https://archive.org/", headers={}, timeout=1)
    assert not list(tmp_path.glob("*.gz"))


def test_evidence_storage_and_hash(tmp_path, monkeypatch):
    import gzip
    import json
    response = requests.Response()
    response.status_code = 200
    response.url = "https://archive.org/wayback/available"
    response._content = b'{"archived_snapshots":{}}'
    response._content_consumed = True
    session = module.EvidenceSession(tmp_path)
    monkeypatch.setattr(session.session, "get", lambda *a, **k: response)
    session.get(response.url, headers={}, timeout=1)
    record = json.loads((tmp_path / "requests.jsonl").read_text())
    assert gzip.decompress((tmp_path / record["evidence_file"]).read_bytes()) == response.content
    assert record["sha256"] == module.digest(response.content)
    assert session.used == sum(p.stat().st_size for p in tmp_path.glob("*.gz"))


def test_total_storage_bound(tmp_path, monkeypatch):
    response = requests.Response()
    response.status_code = 200
    response.url = "https://archive.org/"
    response._content = b"body"
    response._content_consumed = True
    session = module.EvidenceSession(tmp_path)
    monkeypatch.setattr(module, "MAX_STORED", 1)
    monkeypatch.setattr(session.session, "get", lambda *a, **k: response)
    with pytest.raises(requests.RequestException, match="storage limit"):
        session.get(response.url, headers={}, timeout=1)
    assert not list(tmp_path.glob("*.gz"))


def test_checkpoint_resume_preserves_keys_and_source_provenance(tmp_path, monkeypatch):
    import json
    source = tmp_path / "input.csv"
    source.write_text("candidate_url_id,month,full_name,summary_change_type,source_expansion_claim_ids_json\n"
                      "a,11.0,Test Name,new_candidate_url_lead,[]\n")
    calls = []
    def validate(row, **kwargs):
        calls.append(row["validation_key"])
        return {"validation_key": row["validation_key"], "retryable": False,
                "temporal_validation_status": "no_archive_capture",
                "validation_reason": "no_archive_capture", "distance": module.pd.NA}
    monkeypatch.setattr(module.validator, "validate_one_lead", validate)
    monkeypatch.setattr(sys, "argv", ["validate", "--input", str(source), "--output", str(tmp_path / "out")])
    module.main()
    module.main()
    assert calls == ["a"]
    result = json.loads((tmp_path / "out/results.jsonl").read_text())
    assert result["distance"] is None
    assert result["source_claim_ids_json"] == "[]"
    assert result["input_sha256"] == module.digest(source.read_bytes())
