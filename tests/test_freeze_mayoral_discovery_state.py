import hashlib
import json

from scripts.freeze_mayoral_discovery_state import build_manifest


def test_build_manifest_records_hashes_and_row_counts(tmp_path):
    (tmp_path / "table.csv").write_text("a,b\n1,2\n3,4\n", encoding="utf-8")
    (tmp_path / "events.jsonl").write_text('{"x":1}\n\n{"x":2}\n', encoding="utf-8")
    (tmp_path / "items.json").write_text(json.dumps([1, 2, 3]), encoding="utf-8")

    result = build_manifest(["table.csv", "events.jsonl", "items.json"], tmp_path)
    files = {item["path"]: item for item in result["files"]}

    assert files["table.csv"]["records"] == 2
    assert files["table.csv"]["columns"] == 2
    assert files["events.jsonl"]["records"] == 2
    assert files["items.json"]["records"] == 3
    assert files["table.csv"]["sha256"] == hashlib.sha256(
        (tmp_path / "table.csv").read_bytes()
    ).hexdigest()


def test_build_manifest_accepts_latin1_csv(tmp_path):
    (tmp_path / "latin.csv").write_bytes("name\nMüller\n".encode("latin-1"))
    result = build_manifest(["latin.csv"], tmp_path)
    assert result["files"][0]["records"] == 1
