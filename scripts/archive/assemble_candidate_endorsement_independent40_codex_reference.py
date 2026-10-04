"""Assemble a separate AI reference without promoting it to human truth."""
from __future__ import annotations

import argparse
import copy
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts import compare_candidate_endorsement_independent40_codex as comparison

review = comparison.review
SEED_REFERENCE = comparison.OUTPUT / "seed_audit_reviewed_reference.json"
SEED_SHA256 = "54f3fb130bc1a61e5ec7e23d23303fbe13b0a25bafa0f85a9764b3c630cfb2fe"
ADJUDICATED_PARENTS = (4, 7, 12, 14, 15, 16, 18, 20, 21, 27, 28, 31, 32, 39)
REVIEWER_ID = "independent_source_adjudicator"
REVIEWER_MODEL = "gpt-5.6-sol"


def pinned(path, expected):
    if path.is_symlink() or not path.is_file() or comparison.digest(path.read_bytes()) != expected:
        raise ValueError(f"Pinned reference differs: {path}")
    return comparison.strict_json(path)


def authenticate_initial(source_dir, seed_reference_path):
    review.read_parent(1, source_dir)
    parents = [comparison.strict_json(source_dir / "source" / f"parent_{i:02d}.json") for i in range(1, 41)]
    blind = pinned(source_dir / "blind_reviewed_reference.json", comparison.BLIND_SHA256)
    seed = pinned(seed_reference_path, SEED_SHA256)
    for name, frozen in (("blind", blind), ("seed_audit", seed)):
        if comparison.pass_reference(name, parents, source_dir) != frozen:
            raise ValueError(f"Authenticated {name} inventory differs from its pinned reference")
    _, items, _ = comparison.compare_values(parents, blind, seed)
    if tuple(item["parent_index"] for item in items if item["status_membership_differs"]) != ADJUDICATED_PARENTS:
        raise ValueError("The required adjudication parent set differs")
    return parents, blind, seed


def issue_key(issue):
    return tuple(tuple(span) for span in issue["source_spans"]), issue["reason"]


def merge_uncertainties(*passes):
    merged = {}
    for pass_name, issues in passes:
        for issue in issues:
            key = tuple(tuple(span) for span in issue["source_spans"])
            entry = merged.setdefault(key, {"source_spans": copy.deepcopy(issue["source_spans"]), "reasons": []})
            attributed = f"[{pass_name}] {issue['reason']}"
            if attributed not in entry["reasons"]:
                entry["reasons"].append(attributed)
    return [{"source_spans": entry["source_spans"], "reason": "\n".join(entry["reasons"])} for entry in merged.values()]


def authenticate_adjudications(source_dir, parents, blind, seed):
    folder = source_dir / "adjudicated"
    expected = {f"parent_{i:02d}.json" for i in ADJUDICATED_PARENTS} | {"decision_receipt.json"}
    if folder.is_symlink() or not folder.is_dir() or {p.name for p in folder.iterdir()} != expected:
        raise ValueError("Exactly 14 adjudications and their decision receipt are required")
    receipt_path = folder / "decision_receipt.json"
    if receipt_path.is_symlink() or not receipt_path.is_file():
        raise ValueError("Decision receipt must be a regular file")
    receipt = comparison.strict_json(receipt_path)
    comparison.reject_human_claims(receipt)
    if (receipt.get("human_reviewed") is not False or receipt.get("authorizations") != review.AUTHORIZATIONS
            or receipt.get("reviewer_id") != REVIEWER_ID or receipt.get("reviewer_model") != REVIEWER_MODEL
            or receipt.get("parent_indices") != list(ADJUDICATED_PARENTS)
            or receipt.get("initial_blind_reference_sha256") != comparison.BLIND_SHA256
            or receipt.get("initial_seed_audit_reference_sha256") != SEED_SHA256
            or [p.get("parent_index") for p in receipt.get("parents", [])] != list(ADJUDICATED_PARENTS)):
        raise ValueError("Decision receipt identity or authority differs")
    adjudicated, inventory = {}, []
    totals = dict(parents=14, accepted_children=0, uncertain_children=0, uncertainty_blocks=0)
    for entry in receipt["parents"]:
        i = entry["parent_index"]
        parent = parents[i - 1]
        if (entry.get("parent_document_id") != parent["parent_document_id"]
                or entry.get("source_text_sha256") != parent["source_text_sha256"]
                or entry.get("complete_source_reviewed") is not True):
            raise ValueError("Decision receipt parent identity or source coverage differs")
        initial = {"blind": blind["responses"][i - 1], "seed_audit": seed["responses"][i - 1]}
        expected_hashes = {name: reference["review_inventory"][i - 1]["sha256"] for name, reference in (("blind", blind), ("seed_audit", seed))}
        if entry.get("initial_review_hashes") != expected_hashes:
            raise ValueError("Decision receipt initial review pins differ")
        expected_issues = {(name, *issue_key(issue)) for name, response in initial.items() for issue in response["uncertainties"]}
        recorded = set()
        for issue in entry.get("uncertainty_decisions", []):
            if issue.get("disposition") != "retain":
                raise ValueError("This assembly retains all initial source uncertainties")
            recorded.add((issue["pass"], *issue_key(issue)))
        if recorded != expected_issues:
            raise ValueError("Decision receipt does not retain all initial uncertainties")
        for decision in entry.get("decisions", []):
            review.utilities.validate_spans(decision.get("source_spans"), parents[i - 1]["source_text"], "decision")
            if not decision.get("decision") or not decision.get("reason"):
                raise ValueError("Source decision lacks a decision or reason")
            if decision.get("evidence") != comparison.span_evidence(parent, decision["source_spans"]):
                raise ValueError("Decision evidence differs from exact authenticated source segments")
        path = folder / f"parent_{i:02d}.json"
        if path.is_symlink() or not path.is_file():
            raise ValueError("Adjudication must be a regular file")
        value = review.validate_review(comparison.strict_json(path), parents[i - 1])
        comparison.reject_human_claims(value)
        if value["reviewer_id"] != REVIEWER_ID or value["reviewer_model"] != REVIEWER_MODEL:
            raise ValueError("Adjudication reviewer provenance differs")
        actual_hash = comparison.digest(path.read_bytes())
        actual_counts = {"accepted_children": sum(row["endorsement_status"] == "accepted_by_ai_review" for row in value["children"]),
                         "uncertain_children": sum(row["endorsement_status"] == "uncertain" for row in value["children"]),
                         "uncertainty_blocks": len(value["uncertainties"])}
        if entry.get("adjudicated_review_sha256") != actual_hash or entry.get("counts") != actual_counts:
            raise ValueError("Decision receipt adjudication hash or counts differ")
        for key, count in actual_counts.items():
            totals[key] += count
        adjudicated[i] = value
        inventory.append({"parent_index": i, "file": f"adjudicated/{path.name}", "sha256": actual_hash})
    if receipt.get("counts") != totals:
        raise ValueError("Decision receipt aggregate counts differ")
    return adjudicated, receipt, inventory, comparison.digest(receipt_path.read_bytes())


def assemble(*, source_dir=review.OUTPUT, seed_reference_path=SEED_REFERENCE,
             output_folder="final_ai_reference", assembly_status="pending_root_approval"):
    source_dir, seed_reference_path = Path(source_dir), Path(seed_reference_path)
    if (not output_folder or Path(output_folder).name != output_folder or output_folder in {".", ".."}
            or assembly_status not in {"pending_root_approval", "test_only"}):
        raise ValueError("A new local folder and explicit assembly status are required")
    output_dir = source_dir / output_folder
    if output_dir.exists() or output_dir.is_symlink():
        raise ValueError("Assembly output must be a new directory")
    parents, blind, seed = authenticate_initial(source_dir, seed_reference_path)
    adjudicated, receipt, adjudication_inventory, receipt_hash = authenticate_adjudications(source_dir, parents, blind, seed)
    values, assembly_parents = [], []
    counts = dict(parents=40, accepted_children=0, uncertain_children=0, uncertainty_blocks=0, ai_zero_parents=0)
    for i, parent in enumerate(parents, 1):
        a, b = blind["responses"][i - 1], seed["responses"][i - 1]
        if i in adjudicated:
            value = copy.deepcopy(adjudicated[i])
            passes = [("adjudicated", value["uncertainties"]), ("blind", a["uncertainties"]), ("seed_audit", b["uncertainties"])]
            method = "source_adjudication_with_all_initial_uncertainties_retained"
        else:
            identities = lambda response: {review.utilities.identity(row): row["endorsement_status"] for row in response["children"]}
            if identities(a) != identities(b):
                raise ValueError("A consensus parent has unresolved identity or status differences")
            value = {k: copy.deepcopy(v) for k, v in a.items() if k != "metadata"}
            value["reviewer_id"] = "independent_blind_seed_consensus_assembly"
            value["notes"] = ("Assembly preserves blind rows after exact identity and status agreement. "
                              f"Source reviewers: {a['reviewer_id']}, {b['reviewer_id']}. "
                              f"[blind notes] {a['notes']} [seed_audit notes] {b['notes']}")
            passes = [("blind", a["uncertainties"]), ("seed_audit", b["uncertainties"])]
            method = "exact_identity_status_consensus_preserving_blind_rows"
        value["uncertainties"] = merge_uncertainties(*passes)
        review.validate_review(value, parent)
        comparison.reject_human_claims(value)
        values.append(value)
        counts["accepted_children"] += sum(row["endorsement_status"] == "accepted_by_ai_review" for row in value["children"])
        counts["uncertain_children"] += sum(row["endorsement_status"] == "uncertain" for row in value["children"])
        counts["uncertainty_blocks"] += len(value["uncertainties"])
        counts["ai_zero_parents"] += not value["children"] and not value["uncertainties"]
        assembly_parents.append({"parent_index": i, "method": method,
            "source_reviewer_ids": [a["reviewer_id"], b["reviewer_id"]] + ([REVIEWER_ID] if i in adjudicated else []),
            "source_reviewer_models": [REVIEWER_MODEL], "human_reviewed": False})
    # Authenticate and validate every parent before writing any output.
    inventory = []
    for i, value in enumerate(values, 1):
        path = output_dir / "reviews" / f"parent_{i:02d}.json"
        review.utilities.write_json(path, value)
        inventory.append({"parent_index": i, "file": f"{output_folder}/reviews/{path.name}", "sha256": comparison.digest(path.read_bytes())})
    reference = {"version": "independent40_adjudicated_ai_reference_v1",
        "review_status": "complete_adjudicated_ai_review_not_human_truth",
        "source_manifest_sha256": blind["source_manifest_sha256"], "instructions_sha256": blind["instructions_sha256"],
        "review_inventory": inventory, "counts": counts,
        "responses": [{**copy.deepcopy(value), "metadata": parent["metadata"]} for value, parent in zip(values, parents, strict=True)],
        "human_reviewed": False, "authorizations": copy.deepcopy(review.AUTHORIZATIONS),
        "interpretation": "Source adjudication and exact AI consensus remain provisional AI judgments, not human-validated accuracy."}
    reference_path = output_dir / "ai_reviewed_reference.json"
    review.utilities.write_json(reference_path, reference)
    auth = {"version": "independent40_ai_reference_assembly_auth_v1", "assembly_status": assembly_status,
        "blind_reference_sha256": comparison.BLIND_SHA256, "seed_audit_reference_sha256": SEED_SHA256,
        "source_manifest_sha256": blind["source_manifest_sha256"], "instructions_sha256": blind["instructions_sha256"],
        "adjudication_inventory": adjudication_inventory, "decision_receipt_sha256": receipt_hash,
        "final_reference_sha256": comparison.digest(reference_path.read_bytes()), "final_review_inventory": inventory,
        "assembler_sha256": comparison.digest(Path(__file__).read_bytes()), "assembly_parents": assembly_parents,
        "uncertainty_rule": "retain_every_initial_uncertainty_with_exact_span_grouping_and_pass_attribution",
        "human_reviewed": False, "authorizations": copy.deepcopy(review.AUTHORIZATIONS),
        "root_complete_source_review_claimed": False, "human_accuracy_claimed": False}
    review.utilities.write_json(output_dir / "auth.json", auth)
    return auth


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-dir", type=Path, default=review.OUTPUT)
    parser.add_argument("--seed-reference-path", type=Path, default=SEED_REFERENCE)
    parser.add_argument("--output-folder", default="final_ai_reference")
    parser.add_argument("--assembly-status", choices=["pending_root_approval", "test_only"], required=True)
    print(json.dumps(assemble(**vars(parser.parse_args())), indent=2))
