"""Tests for scripts/build_mayoral_page_check_table.py: label order, hand decisions and same-text labels."""
import csv
import json
import subprocess
import sys
from pathlib import Path

import pandas as pd

REPO = Path(__file__).resolve().parents[1]


def write_jsonl(path, rows):
    path.write_text("".join(json.dumps(r) + "\n" for r in rows))


def test_merge_order_overrides_and_same_text(tmp_path):
    write_jsonl(tmp_path / "items.jsonl", [{"item_id": i} for i in ["a", "b", "c"]])
    write_jsonl(tmp_path / "v1.jsonl", [
        {"item_id": "a", "status": "ok", "label": "earlier_campaign", "prompt_version": "v1"},
        {"item_id": "b", "status": "ok", "label": "this_campaign", "prompt_version": "v1"},
        {"item_id": "c", "status": "parse_error"}])
    write_jsonl(tmp_path / "v3.jsonl", [
        {"item_id": "a", "status": "ok", "label": "earlier_campaign", "race_year": 2019, "prompt_version": "v3"},
        {"item_id": "b", "status": "ok", "label": "other_office", "race_year": None, "prompt_version": "v3"}])
    (tmp_path / "hand.json").write_text(json.dumps([{"page_id": "a", "label": "this_campaign", "reason": "x"}]))
    pd.DataFrame({"page_id": ["a", "b", "c", "d", "e"], "candidate_election_id": ["f1", "f1", "f1", "f1", "f2"],
                  "text_sha256": ["s1", "s2", "s3", "s2", "s2"]}).to_parquet(tmp_path / "corpus.parquet")
    subprocess.run([sys.executable, "scripts/build_mayoral_page_check_table.py", "--items", tmp_path / "items.jsonl",
                    "--labels", tmp_path / "v1.jsonl", tmp_path / "v3.jsonl", "--overrides", tmp_path / "hand.json",
                    "--corpus", tmp_path / "corpus.parquet", "--output", tmp_path / "out.csv"],
                   cwd=REPO, check=True, capture_output=True)
    out = {r["page_id"]: r for r in csv.DictReader((tmp_path / "out.csv").open())}
    assert out["a"]["label"] == "this_campaign" and out["a"]["prompt_version"] == "opus-hand-check-2026-10-17"
    assert out["b"]["label"] == "other_office"  # the later file replaces the earlier label
    assert "c" not in out  # a parse error gives no label
    assert out["d"]["label"] == "other_office" and out["d"]["prompt_version"] == "same-text-as-labeled-page"
    assert "e" not in out  # same text, but another finalist
