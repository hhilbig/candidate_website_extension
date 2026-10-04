"""Test synthetic adjudications in temporary packages, not final AI decisions."""
import copy
import json
import shutil

import pytest

from scripts import assemble_candidate_endorsement_independent40_codex_reference as app


@pytest.fixture(scope="module")
def initial():
    if not app.SEED_REFERENCE.exists() or not (app.review.OUTPUT / "blind_reviewed_reference.json").exists():
        pytest.skip("Private references are absent")
    return app.authenticate_initial(app.review.OUTPUT, app.SEED_REFERENCE)


def synthetic_adjudications(folder, initial):
    parents, blind, seed = initial
    entries = []
    totals = dict(parents=14, accepted_children=0, uncertain_children=0, uncertainty_blocks=0)
    for i in app.ADJUDICATED_PARENTS:
        value = {k: copy.deepcopy(v) for k, v in blind["responses"][i - 1].items() if k != "metadata"}
        value.update(reviewer_id=app.REVIEWER_ID, reviewer_model=app.REVIEWER_MODEL,
                     notes="Synthetic test adjudication. Not a final source decision.")
        app.review.utilities.write_json(folder / f"parent_{i:02d}.json", value)
        counts = {"accepted_children": sum(row["endorsement_status"] == "accepted_by_ai_review" for row in value["children"]),
                  "uncertain_children": sum(row["endorsement_status"] == "uncertain" for row in value["children"]),
                  "uncertainty_blocks": len(value["uncertainties"])}
        for key, count in counts.items():
            totals[key] += count
        spans = value["children"][0]["support_spans"]
        entries.append({"parent_index": i,
            "parent_document_id": parents[i - 1]["parent_document_id"],
            "source_text_sha256": parents[i - 1]["source_text_sha256"], "complete_source_reviewed": True,
            "adjudicated_review_sha256": app.comparison.digest((folder / f"parent_{i:02d}.json").read_bytes()),
            "counts": counts,
            "initial_review_hashes": {"blind": blind["review_inventory"][i - 1]["sha256"],
                                      "seed_audit": seed["review_inventory"][i - 1]["sha256"]},
            "decisions": [{"source_spans": spans, "decision": "synthetic_test", "reason": "Test only",
                           "evidence": app.comparison.span_evidence(parents[i - 1], spans)}],
            "uncertainty_decisions": [{**copy.deepcopy(issue), "pass": name, "disposition": "retain"}
                for name, reference in (("blind", blind), ("seed_audit", seed))
                for issue in reference["responses"][i - 1]["uncertainties"]]})
    app.review.utilities.write_json(folder / "decision_receipt.json", {
        "reviewer_id": app.REVIEWER_ID, "reviewer_model": app.REVIEWER_MODEL,
        "parent_indices": list(app.ADJUDICATED_PARENTS), "initial_blind_reference_sha256": app.comparison.BLIND_SHA256,
        "initial_seed_audit_reference_sha256": app.SEED_SHA256, "counts": totals,
        "parents": entries, "human_reviewed": False, "authorizations": copy.deepcopy(app.review.AUTHORIZATIONS)})


def test_initial_pins_and_membership_difference_set(initial):
    parents, blind, seed = initial
    assert len(parents) == len(blind["responses"]) == len(seed["responses"]) == 40
    assert app.comparison.digest(app.SEED_REFERENCE.read_bytes()) == app.SEED_SHA256
    assert app.comparison.digest((app.review.OUTPUT / "blind_reviewed_reference.json").read_bytes()) == app.comparison.BLIND_SHA256


def test_span_grouping_preserves_pass_reasons_and_distinct_issues():
    one = {"source_spans": [[1, 3]], "reason": "First reason"}
    other = {"source_spans": [[1, 3]], "reason": "Second reason"}
    distinct = {"source_spans": [[4, 6]], "reason": "Distinct issue"}
    result = app.merge_uncertainties(("blind", [one, one, distinct]), ("seed_audit", [other]))
    assert result == [{"source_spans": [[1, 3]], "reason": "[blind] First reason\n[seed_audit] Second reason"},
                      {"source_spans": [[4, 6]], "reason": "[blind] Distinct issue"}]
    assert one["reason"] == "First reason"


@pytest.mark.parametrize("mutation", ["missing", "extra", "human", "reviewer", "receipt_pin", "receipt_issue", "dispose"])
def test_adjudication_authority_and_complete_inventory(initial, tmp_path, mutation):
    folder = tmp_path / "adjudicated"
    synthetic_adjudications(folder, initial)
    if mutation == "missing":
        (folder / "parent_04.json").unlink()
    elif mutation == "extra":
        (folder / "parent_01.json").write_text("{}")
    elif mutation in {"human", "reviewer"}:
        path = folder / "parent_04.json"
        value = json.loads(path.read_text())
        value["human_reviewed" if mutation == "human" else "reviewer_model"] = True if mutation == "human" else "unknown"
        path.write_text(json.dumps(value))
    else:
        path = folder / "decision_receipt.json"
        value = json.loads(path.read_text())
        if mutation == "receipt_pin":
            value["parents"][0]["initial_review_hashes"]["blind"] = "0" * 64
        else:
            entry = next(p for p in value["parents"] if p["uncertainty_decisions"])
            if mutation == "receipt_issue":
                entry["uncertainty_decisions"].pop()
            else:
                entry["uncertainty_decisions"][0]["disposition"] = "resolved"
        path.write_text(json.dumps(value))
    with pytest.raises(ValueError):
        app.authenticate_adjudications(tmp_path, *initial)


def test_changed_seed_pin_fails(initial, tmp_path):
    path = tmp_path / "changed_seed.json"
    path.write_text("{}")
    with pytest.raises(ValueError, match="Pinned reference differs"):
        app.authenticate_initial(app.review.OUTPUT, path)


@pytest.mark.parametrize("mutation", ["blind_pin", "seed_pin", "indices", "parent_id", "source_hash", "coverage", "review_hash", "parent_counts", "total_counts", "evidence_text", "evidence_span"])
def test_receipt_bindings_reject_tampering(initial, tmp_path, mutation):
    folder = tmp_path / "adjudicated"
    synthetic_adjudications(folder, initial)
    path = folder / "decision_receipt.json"
    receipt = json.loads(path.read_text())
    entry = receipt["parents"][0]
    if mutation in {"blind_pin", "seed_pin"}:
        receipt["initial_blind_reference_sha256" if mutation == "blind_pin" else "initial_seed_audit_reference_sha256"] = "0" * 64
    elif mutation == "indices":
        receipt["parent_indices"].reverse()
    elif mutation in {"parent_id", "source_hash", "review_hash"}:
        key = {"parent_id": "parent_document_id", "source_hash": "source_text_sha256", "review_hash": "adjudicated_review_sha256"}[mutation]
        entry[key] = "0" * 64
    elif mutation == "coverage":
        entry["complete_source_reviewed"] = False
    elif mutation in {"parent_counts", "total_counts"}:
        (entry if mutation == "parent_counts" else receipt)["counts"]["accepted_children"] += 1
    else:
        evidence = entry["decisions"][0]["evidence"][0]
        if mutation == "evidence_text":
            evidence["source_text"] += "changed"
        else:
            evidence["source_span"][0] += 1
    path.write_text(json.dumps(receipt))
    with pytest.raises(ValueError):
        app.authenticate_adjudications(tmp_path, *initial)


def test_temporary_assembly_preserves_rows_issues_and_pins(initial, tmp_path):
    source = tmp_path / "test_only_source"
    source.mkdir()
    for name in ("source", "blind", "seed_audit"):
        shutil.copytree(app.review.OUTPUT / name, source / name)
    for name in ("instructions.json", "instruction_provenance.json", "manifest.json", "blind_reviewed_reference.json"):
        shutil.copy2(app.review.OUTPUT / name, source / name)
    synthetic_adjudications(source / "adjudicated", initial)
    auth = app.assemble(source_dir=source, seed_reference_path=app.SEED_REFERENCE,
                        output_folder="test_only_assembled", assembly_status="test_only")
    output = source / "test_only_assembled"
    reference_path = output / "ai_reviewed_reference.json"
    reference = json.loads(reference_path.read_text())
    assert auth["assembly_status"] == "test_only"
    assert auth["human_reviewed"] is False
    assert auth["root_complete_source_review_claimed"] is False
    assert auth["final_reference_sha256"] == app.comparison.digest(reference_path.read_bytes())
    assert len(reference["review_inventory"]) == len(reference["responses"]) == 40
    assert len(auth["adjudication_inventory"]) == 14
    assert len(list((output / "reviews").iterdir())) == 40
    for i, response in enumerate(reference["responses"], 1):
        a, b = initial[1]["responses"][i - 1], initial[2]["responses"][i - 1]
        assert response["children"] == a["children"]
        assert response["human_reviewed"] is False
        assert response["reviewer_model"] == "gpt-5.6-sol"
        for name, original in (("blind", a), ("seed_audit", b)):
            for issue in original["uncertainties"]:
                merged = next(issue2 for issue2 in response["uncertainties"] if issue2["source_spans"] == issue["source_spans"])
                assert f"[{name}] {issue['reason']}" in merged["reason"]
        entry = reference["review_inventory"][i - 1]
        path = source / entry["file"]
        assert app.comparison.digest(path.read_bytes()) == entry["sha256"]
        assert json.loads(path.read_text()) == {k: v for k, v in response.items() if k != "metadata"}
    assert app.comparison.digest((source / "blind_reviewed_reference.json").read_bytes()) == app.comparison.BLIND_SHA256
    assert app.comparison.digest(app.SEED_REFERENCE.read_bytes()) == app.SEED_SHA256
    with pytest.raises(ValueError, match="new directory"):
        app.assemble(source_dir=source, output_folder="test_only_assembled", assembly_status="test_only")
