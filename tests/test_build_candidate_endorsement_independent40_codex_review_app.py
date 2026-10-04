"""Test temporary apps without approving a final independent AI reference."""
import copy
import json
import re

import pytest

from scripts import build_candidate_endorsement_independent40_codex_review_app as app

REFERENCE = app.review.OUTPUT / "blind_reviewed_reference.json"
REFERENCE_SHA256 = "8d6f5dbad5fbd2dc392bc1ad45f0048bad5fac4d9552d5f677e08d293cebcb79"


@pytest.fixture(scope="module")
def inputs():
    if not app.SOURCE_TASK.exists() or not REFERENCE.exists():
        pytest.skip("Private review inputs are absent")
    manifest_hash = app.base.sha256_file(app.review.OUTPUT / "manifest.json")
    source, reference = app.load_inputs(reference_path=REFERENCE,
        reference_sha256=REFERENCE_SHA256, source_manifest_sha256=manifest_hash)
    return source, reference, manifest_hash


def task_options(inputs):
    return dict(reference_sha256=REFERENCE_SHA256, source_manifest_sha256=inputs[2],
        task_id="independent40_codex_adapter_test", task_revision="independent40_codex_adapter_test_v1",
        reference_approval="test_only")


def test_seeds_and_blocks_remain_pending(inputs):
    source, reference, _ = inputs
    original = copy.deepcopy(source)
    task = app.build_task(source, reference, **task_options(inputs))
    assert source == original
    assert task == app.build_task(source, reference, **task_options(inputs))
    children = [c for p in task["parents"] for c in p["ai_children"]]
    blocks = [b for p in task["parents"] for b in p["ai_unresolved_blocks"]]
    assert len(children) == len({c["provisional_child_id"] for c in children}) == 2581
    assert len(blocks) == len({b["unresolved_block_id"] for b in blocks}) == 58
    assert all(c["human_confirmed"] is False for c in children)
    assert all(c["ai_review_metadata"]["human_reviewed"] is False for c in children)
    assert all(b["human_reviewed"] is False for b in blocks)
    assert task["human_reviewed"] is False
    assert task["authorizations"] == app.base.REVIEW_AUTHORIZATIONS
    assert task["input_provenance"]["reference_approval"] == "test_only"
    assert task["input_provenance"]["original_model_child_count"] == 2489
    assert task["input_provenance"]["original_model_provisional_draft_sha256"] == source["input_provenance"]["provisional_draft_sha256"]
    assert sum(c["ai_review_metadata"]["endorsement_status"] == "uncertain" for c in children) == 209
    for parent, old, response in zip(task["parents"], source["parents"], reference["responses"], strict=True):
        assert parent["parent_source_text"] == old["parent_source_text"]
        assert parent["human_parent_confirmed"] is False
        n = len(old["ai_unresolved_blocks"])
        assert parent["ai_unresolved_blocks"][:n] == old["ai_unresolved_blocks"]
        for block, issue in zip(parent["ai_unresolved_blocks"][n:], response["uncertainties"], strict=True):
            assert block["source_spans"] == issue["source_spans"]
            assert block["reason"] == issue["reason"]
            assert issue["reason"] in task["instructions"]


@pytest.mark.parametrize("mutation", ["source", "human_parent", "human_child", "human_block", "authority", "count", "manifest", "order", "status", "duplicate"])
def test_rejects_changes(inputs, mutation):
    source, reference, manifest_hash = copy.deepcopy(inputs)
    if mutation == "source":
        source["parents"][0]["parent_source_text"] += "changed"
    elif mutation == "human_parent":
        source["parents"][0]["human_parent_confirmed"] = True
    elif mutation == "human_child":
        reference["responses"][0]["children"][0]["human_reviewed"] = True
    elif mutation == "human_block":
        next(p["ai_unresolved_blocks"][0] for p in source["parents"] if p["ai_unresolved_blocks"])["human_reviewed"] = True
    elif mutation == "authority":
        reference["authorizations"]["human_truth"] = True
    elif mutation == "count":
        reference["counts"]["accepted_children"] -= 1
    elif mutation == "manifest":
        manifest_hash = "0" * 64
    elif mutation == "order":
        reference["responses"][0], reference["responses"][1] = reference["responses"][1], reference["responses"][0]
    elif mutation == "status":
        reference["responses"][0]["children"][0]["endorsement_status"] = "human_accepted"
    else:
        reference["responses"][0]["children"].append(copy.deepcopy(reference["responses"][0]["children"][0]))
    with pytest.raises(ValueError):
        app.validate_inputs(source, reference, manifest_hash)


def test_separate_identity_and_approval_required(inputs):
    source, reference, _ = inputs
    for change in [dict(task_id=source["task_id"]), dict(task_revision=source["task_revision"]), dict(reference_approval="pending")]:
        options = {**task_options(inputs), **change}
        with pytest.raises(ValueError):
            app.build_task(source, reference, **options)


def test_hash_failure_precedes_conversion(tmp_path):
    path = tmp_path / "changed.json"
    path.write_text("{}")
    with pytest.raises(ValueError, match="Pinned input hash"):
        app.load_inputs(reference_path=path, reference_sha256="0" * 64,
            source_manifest_sha256="0" * 64, source_task_path=path)


@pytest.mark.parametrize("mutation", ["inventory_hash", "inventory_path", "instructions", "metadata"])
def test_source_and_inventory_authentication(inputs, tmp_path, mutation):
    reference = copy.deepcopy(inputs[1])
    if mutation == "inventory_hash":
        reference["review_inventory"][0]["sha256"] = "0" * 64
    elif mutation == "inventory_path":
        reference["review_inventory"][0]["file"] = "../../outside.json"
    elif mutation == "instructions":
        reference["instructions_sha256"] = "0" * 64
    else:
        reference["responses"][0]["metadata"]["source_urls"] = ["changed"]
    path = tmp_path / "repinned_but_invalid_reference.json"
    path.write_bytes(app.base.canonical_bytes(reference))
    output = tmp_path / "must_not_exist"
    options = {**task_options(inputs), "reference_sha256": app.base.sha256_file(path)}
    with pytest.raises(ValueError):
        app.build_files(output_dir=output, reference_path=path, **options)
    assert not output.exists()


def test_temporary_build_and_export_binding(inputs, tmp_path):
    output = tmp_path / "test_only_app"
    options = dict(output_dir=output, reference_path=REFERENCE, **task_options(inputs))
    auth = app.build_files(**options)
    raw = (output / "task.json").read_bytes()
    task = json.loads(raw)
    html = (output / "index.html").read_text()
    embedded = re.search(r'<script id="taskData" type="application/json">(.*?)</script>', html, re.S)
    assert embedded and json.loads(embedded.group(1)) == task
    assert app.base.sha256_bytes(raw) == auth["task_sha256"]
    assert app.base.sha256_file(output / "index.html") == auth["html_sha256"]
    assert auth["reference_approval"] == "test_only"
    assert "confirmed:false" in html
    assert "parent_confirmed:false" in html
    assert "reviewed_unresolved_block_ids:[]" in html
    with pytest.raises(ValueError):
        app.base.validate_completed_results(inputs[1], task=task, task_sha256=auth["task_sha256"])
    with pytest.raises(ValueError, match="must not already exist"):
        app.build_files(**options)
    assert app.base.sha256_file(REFERENCE) == REFERENCE_SHA256
