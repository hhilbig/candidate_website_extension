"""Check the bounded Codex app adapter and its authority limits."""
import copy
import json
import re
from pathlib import Path

import pytest

from scripts import build_candidate_endorsement_codex_review_app as app


@pytest.fixture(scope="module")
def inputs():
    if not app.SOURCE_TASK.exists() or not app.AI_REVIEW.exists():
        pytest.skip("Private pinned review artifacts are absent")
    return app.load_inputs()


def test_pinned_inputs_and_inventory(inputs):
    source, reference = inputs
    assert app.base.sha256_file(app.SOURCE_TASK) == app.SOURCE_TASK_SHA256
    assert app.base.sha256_file(app.AI_REVIEW) == app.AI_REVIEW_SHA256
    assert len(reference["review_file_inventory"]) == 131
    assert sum(len(p["ai_unresolved_blocks"]) for p in source["parents"]) == 17


def test_all_ai_children_remain_unconfirmed(inputs):
    source, reference = inputs
    original = copy.deepcopy(source)
    task = app.build_task(source, reference)
    assert source == original
    assert task["task_id"] != source["task_id"]
    assert task["task_revision"] != source["task_revision"]
    assert task["human_reviewed"] is False
    assert task["authorizations"] == app.base.REVIEW_AUTHORIZATIONS
    assert task["input_provenance"]["provisional_draft_sha256"] == app.AI_REVIEW_SHA256
    children = [c for p in task["parents"] for c in p["ai_children"]]
    assert len(children) == 1960
    assert len({c["provisional_child_id"] for c in children}) == 1960
    assert all(c["human_confirmed"] is False for c in children)
    assert all(c["ai_review_metadata"]["human_reviewed"] is False for c in children)
    assert sum(c["ai_review_metadata"]["endorsement_status"] == "uncertain" for c in children) == 867
    for parent, reviewed in zip(task["parents"], reference["responses"], strict=True):
        assert parent["human_parent_confirmed"] is False
        for seed, row in zip(parent["ai_children"], reviewed["children"], strict=True):
            assert all(seed[k] == row[k] for k in ("entity_type", "name_spans", "support_spans"))


def test_every_issue_is_a_separate_pending_block(inputs):
    source, reference = inputs
    task = app.build_task(source, reference)
    blocks = [b for p in task["parents"] for b in p["ai_unresolved_blocks"]]
    assert len(blocks) == len({b["unresolved_block_id"] for b in blocks}) == 88
    assert all(b["human_reviewed"] is False for b in blocks)
    for parent, original, reviewed in zip(task["parents"], source["parents"], reference["responses"], strict=True):
        n = len(original["ai_unresolved_blocks"])
        assert parent["ai_unresolved_blocks"][:n] == original["ai_unresolved_blocks"]
        for block, issue in zip(parent["ai_unresolved_blocks"][n:], reviewed["uncertainties"], strict=True):
            assert block["source_spans"] == issue["source_spans"]
            assert block["reason"] == issue["reason"]
            assert issue["reason"] in task["instructions"]
            app.base._validate_spans([block["block_span"]], parent["parent_source_text"], label="issue", allow_empty=False)
    assert task == app.build_task(source, reference)


@pytest.mark.parametrize("mutation", ["source", "parent_human", "child_human", "block_human", "count", "authority"])
def test_rejects_invalid_inputs(inputs, mutation):
    source, reference = copy.deepcopy(inputs)
    if mutation == "source":
        source["parents"][0]["parent_source_text"] += "modified"
    elif mutation == "parent_human":
        reference["responses"][0]["human_reviewed"] = True
    elif mutation == "child_human":
        reference["responses"][0]["children"][0]["human_reviewed"] = True
    elif mutation == "block_human":
        next(p["ai_unresolved_blocks"][0] for p in source["parents"] if p["ai_unresolved_blocks"])["human_reviewed"] = True
    elif mutation == "count":
        reference["counts"]["reviewed_children"] = 1959
    else:
        reference["authorizations"]["human_truth"] = True
    with pytest.raises(ValueError):
        app.build_task(source, reference)


def test_hash_authentication_fails_before_conversion(tmp_path):
    path = tmp_path / "changed.json"
    path.write_text("{}")
    with pytest.raises(ValueError, match="Pinned input hash"):
        app.load_inputs(source_path=path)


def test_build_and_embedded_task(inputs, tmp_path):
    output = tmp_path / "review"
    auth = app.build_files(output)
    task_raw = (output / "task.json").read_bytes()
    task = json.loads(task_raw)
    html = (output / "index.html").read_text()
    embedded = re.search(r'<script id="taskData" type="application/json">(.*?)</script>', html, re.S)
    assert embedded and json.loads(embedded.group(1)) == task
    assert app.base.sha256_bytes(task_raw) == auth["task_sha256"]
    assert app.base.sha256_file(output / "index.html") == auth["html_sha256"]
    assert auth["authorizations"] == app.base.REVIEW_AUTHORIZATIONS
    assert "confirmed:false" in html
    assert "parent_confirmed:false" in html
    assert "reviewed_unresolved_block_ids:[]" in html
    assert "accepted_by_ai_review" in task["instructions"]
    assert "AI uncertain rows:" in task["instructions"]
    assert "every_original_failed_block_and_every_codex_source_issue" in task["contract"]["unresolved_block_rule"]
    with pytest.raises(ValueError, match="must not already exist"):
        app.build_files(output)
    with pytest.raises(ValueError):
        app.base.validate_completed_results(inputs[1], task=task, task_sha256=auth["task_sha256"])
