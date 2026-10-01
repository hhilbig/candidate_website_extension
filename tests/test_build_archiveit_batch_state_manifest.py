import json

import pytest

from scripts.build_archiveit_batch_state_manifest import build_manifest


def _fixture(tmp_path):
    collections = [{"collection_id": i} for i in range(1, 21)]
    config = tmp_path / "config.json"
    config.write_text(json.dumps({"batch_id": "b", "collections": collections}))
    payloads = tmp_path / "payloads"
    payloads.mkdir()
    for collection in collections:
        (payloads / f"{collection['collection_id']}.json").write_text("{}")
    artifact = tmp_path / "out.csv"
    artifact.write_text("id\n1\n2\n")
    return config, payloads, artifact


def test_manifest_freezes_exact_payloads_hashes_and_csv_rows(tmp_path):
    config, payloads, artifact = _fixture(tmp_path)
    manifest = build_manifest(config, payloads, [artifact], tmp_path)
    assert manifest["collection_count"] == 20
    assert len(manifest["files"]) == 22
    record = next(row for row in manifest["files"] if row["path"] == "out.csv")
    assert record["csv_rows"] == 2
    assert len(record["sha256"]) == 64
    assert manifest["serper_requests"] == 0


def test_manifest_rejects_missing_payload(tmp_path):
    config, payloads, artifact = _fixture(tmp_path)
    (payloads / "20.json").unlink()
    with pytest.raises(ValueError, match="Payload collection IDs disagree"):
        build_manifest(config, payloads, [artifact], tmp_path)
