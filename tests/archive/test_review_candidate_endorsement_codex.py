import importlib.util
import copy
import json
from pathlib import Path

import pytest

spec = importlib.util.spec_from_file_location("codex_review", Path(__file__).resolve().parents[1] / "scripts/review_candidate_endorsement_codex.py")
review = importlib.util.module_from_spec(spec)
spec.loader.exec_module(review)


def parent():
    text = "Endorsers\nAnn and Bob Smith\n"
    return {"parent_index": 1, "parent_document_id": "p1", "source_text": text,
            "source_text_sha256": review.digest(text.encode()), "lines": review.source_lines(text)}


def result():
    p = parent()
    return dict(parent_index=1, parent_document_id="p1", source_text_sha256=p["source_text_sha256"],
                review_status="ai_reviewed", human_reviewed=False, complete_source_reviewed=True,
                reviewer_id="test", uncertainties=[], children=[review.child(p, "individual", parts=[(2, "Ann"), (2, "Smith")], support=[(1, 2)])])


def test_projection_shared_surname():
    p = parent()
    x = review.validate_review(result(), p)
    assert [p["source_text"][a:b] for a, b in x["children"][0]["name_spans"]] == ["Ann", "Smith"]


@pytest.mark.parametrize("change", ["hash", "span", "human", "coverage", "duplicate"])
def test_reject_invalid_review(change):
    x = result()
    if change == "hash":
        x["source_text_sha256"] = "bad"
    elif change == "span":
        x["children"][0]["name_spans"] = [[0, 1000]]
    elif change == "human":
        x["human_reviewed"] = True
    elif change == "coverage":
        x["complete_source_reviewed"] = False
    else:
        x["children"] *= 2
    with pytest.raises(ValueError):
        review.validate_review(x, parent())


def test_wrong_name_fails():
    with pytest.raises(ValueError, match="not exact"):
        review.name_span(parent(), 2, "Carol")


@pytest.fixture
def prepared(tmp_path, monkeypatch):
    monkeypatch.setattr(review, "ROOT", tmp_path)
    monkeypatch.setattr(review, "OUTPUT", tmp_path / "review")
    task = {"parents": []}
    for i in range(1, 41):
        p = parent()
        task["parents"].append({"parent_document_id": f"p{i}", "parent_source_text": p["source_text"],
                                "source_text_sha256": p["source_text_sha256"], "metadata": {"candidate": f"c{i}"},
                                "ai_children": result()["children"], "ai_unresolved_blocks": []})
    data = json.dumps(task).encode()
    path = tmp_path / "task.json"
    path.write_bytes(data)
    monkeypatch.setattr(review, "TASK", path)
    monkeypatch.setattr(review, "TASK_SHA256", review.digest(data))
    review.prepare()
    return task


def test_prepared_source_tampering_fails(prepared):
    path = review.OUTPUT / "source/parent_01.json"
    p = json.loads(path.read_text())
    p["source_text"] += "changed"
    p["source_text_sha256"] = review.digest(p["source_text"].encode())
    path.write_text(json.dumps(p))
    with pytest.raises(ValueError, match="source identity"):
        review.read_parent(1)


def test_assembly_requires_all_parent_reviews(prepared):
    with pytest.raises(ValueError, match="two full-source"):
        review.assemble()


def test_assembly_authority_and_disagreement_gate(prepared):
    for i in range(1, 41):
        children = result()["children"]
        review.write_review(i, children, "blind", pass_name="blind")
        review.write_review(i, children if i != 1 else [], "seed", pass_name="seed_audit")
    with pytest.raises(ValueError, match="disagreement decision"):
        review.assemble()
    review.write_review(1, [], "primary", pass_name="adjudicated", notes="Candidate identity is uncertain.",
                        uncertainties=[{"source_spans": [[9, 26]], "reason": "Uncertain endorsement context"}])
    review.assemble()
    x = json.loads((review.OUTPUT / "ai_reviewed_results.json").read_text())
    assert x["human_reviewed"] is False
    assert x["review_status"] == "ai_reviewed"
    assert x["parent_count"] == 40
    assert x["counts"]["reviewed_children"] == 39
    assert x["responses"][0]["parent_disposition"] == "unclear"
    assert all(not row["human_reviewed"] for p in x["responses"] for row in p["children"])
    assert x["authorizations"]["human_truth"] is False
    assert prepared["parents"][0]["ai_children"] == result()["children"]


def test_revision_preference_and_uncertain_parent(prepared):
    for i in range(1, 41):
        review.write_review(i, result()["children"], "blind", pass_name="blind")
        review.write_review(i, result()["children"], "seed", pass_name="seed_audit")
    review.write_review(1, [], "primary", pass_name="adjudicated", notes="Earlier decision.")
    review.write_review(1, result()["children"], "primary", pass_name="adjudicated_v2", notes="Revised uncertainty scope.",
                        uncertainties=[{"source_spans": [[9, 26]], "reason": "No explicit endorsement heading"}])
    review.assemble()
    x = json.loads((review.OUTPUT / "ai_reviewed_results_v2.json").read_text())
    assert x["format"].endswith("v2")
    assert x["counts"]["reviewed_children"] == 40
    assert x["responses"][0]["parent_disposition"] == "unclear"
    assert x["responses"][0]["children"][0]["endorsement_status"] == "uncertain"
    assert (review.OUTPUT / "adjudicated/parent_01.json").exists()
