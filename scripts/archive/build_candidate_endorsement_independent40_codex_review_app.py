"""Build a separate human task from an explicitly pinned independent AI reference."""
from __future__ import annotations

import argparse
import copy
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
from scripts import build_candidate_endorsement_round4_human_review_app as base
from scripts import review_candidate_endorsement_independent40_codex as review

SOURCE_TASK = ROOT / "build/private_candidate_endorsement_independent40_model_review_20260914a/task.json"
SOURCE_TASK_SHA256 = "483f88f4bbb557c9252990cff9192afd2e5cabe6aa34b8d97358c6d6dfada1e5"
BUILDER_VERSION = "independent40_codex_human_review_adapter_v1"


def _pinned_json(path: Path, expected: str) -> dict:
    if type(expected) is not str or len(expected) != 64 or any(c not in "0123456789abcdef" for c in expected):
        raise ValueError("An explicit SHA-256 pin is required")
    if path.is_symlink() or not path.is_file():
        raise ValueError(f"Input must be a regular file: {path}")
    raw = path.read_bytes()
    if base.sha256_bytes(raw) != expected:
        raise ValueError(f"Pinned input hash differs: {path}")
    return base.strict_json_bytes(raw, label=str(path))


def validate_inputs(task: dict, reference: dict, source_manifest_sha256: str) -> dict:
    if (task.get("parent_count") != 40 or len(task.get("parents", [])) != 40
            or task.get("authorizations") != base.REVIEW_AUTHORIZATIONS
            or reference.get("human_reviewed") is not False
            or reference.get("authorizations") != review.AUTHORIZATIONS
            or reference.get("source_manifest_sha256") != source_manifest_sha256
            or len(reference.get("responses", [])) != 40):
        raise ValueError("Input census, authority, or source manifest differs")
    counts = dict(parents=40, accepted_children=0, uncertain_children=0,
                  uncertainty_blocks=0, ai_zero_parents=0)
    seen = set()
    for index, (parent, response) in enumerate(zip(task["parents"], reference["responses"], strict=True), 1):
        text = parent["parent_source_text"]
        if (parent["parent_document_id"] in seen or parent["human_parent_confirmed"] is not False
                or parent["source_n_chars"] != len(text)
                or base.sha256_bytes(text.encode()) != parent["source_text_sha256"]
                or {k: v for k, v in response.get("metadata", {}).items() if k != "source_urls"} != parent["metadata"]):
            raise ValueError(f"Parent identity, source, or authority differs: {index}")
        seen.add(parent["parent_document_id"])
        prepared = {"parent_index": index, "parent_document_id": parent["parent_document_id"],
                    "source_text_sha256": parent["source_text_sha256"], "source_text": text}
        review.validate_review({k: v for k, v in response.items() if k != "metadata"}, prepared)
        for row in response["children"]:
            counts["accepted_children" if row["endorsement_status"] == "accepted_by_ai_review" else "uncertain_children"] += 1
        counts["uncertainty_blocks"] += len(response["uncertainties"])
        counts["ai_zero_parents"] += not response["children"] and not response["uncertainties"]
        for row in parent["ai_children"]:
            if row.get("human_confirmed") is not False:
                raise ValueError("Original model child has human confirmation")
        for block in parent["ai_unresolved_blocks"]:
            if block.get("human_reviewed") is not False:
                raise ValueError("Original model block has human confirmation")
            base._validate_spans([block["block_span"]], text, label="model block", allow_empty=False)
    if any(reference.get("counts", {}).get(key) != value for key, value in counts.items()):
        raise ValueError("Reference counts differ")
    if (sum(len(p["ai_children"]) for p in task["parents"]) != 2489
            or sum(len(p["ai_unresolved_blocks"]) for p in task["parents"]) != 11):
        raise ValueError("Original model seed or block counts differ")
    return counts


def load_inputs(*, reference_path: Path, reference_sha256: str,
                source_manifest_sha256: str, source_task_path: Path = SOURCE_TASK,
                source_dir: Path = review.OUTPUT) -> tuple[dict, dict]:
    task = _pinned_json(source_task_path, SOURCE_TASK_SHA256)
    reference = _pinned_json(reference_path, reference_sha256)
    manifest = _pinned_json(source_dir / "manifest.json", source_manifest_sha256)
    # This helper authenticates all prepared sources and the original selection.
    review.read_parent(1, source_dir)
    if (reference.get("instructions_sha256") != base.sha256_file(source_dir / "instructions.json")
            or len(reference.get("review_inventory", [])) != 40):
        raise ValueError("Instruction pin or review inventory differs")
    for index, (parent, response, entry) in enumerate(zip(task["parents"], reference["responses"], reference["review_inventory"], strict=True), 1):
        prepared = base.strict_json_bytes((source_dir / f"source/parent_{index:02d}.json").read_bytes(), label="prepared source")
        if (manifest["parents"][index - 1]["parent_document_id"] != parent["parent_document_id"]
                or prepared["source_text"] != parent["parent_source_text"]
                or prepared["metadata"] != response.get("metadata")):
            raise ValueError("Source task differs from the independent source authority")
        path = (source_dir / entry["file"]).resolve()
        if (entry.get("parent_index") != index or not path.is_relative_to(source_dir.resolve())
                or (source_dir / entry["file"]).is_symlink()):
            raise ValueError("Review inventory path or identity differs")
        value = _pinned_json(path, entry["sha256"])
        if value != {k: v for k, v in response.items() if k != "metadata"}:
            raise ValueError("Review inventory content differs")
    validate_inputs(task, reference, source_manifest_sha256)
    return task, reference


def build_task(source: dict, reference: dict, *, reference_sha256: str,
               source_manifest_sha256: str, task_id: str, task_revision: str,
               reference_approval: str) -> dict:
    counts = validate_inputs(source, reference, source_manifest_sha256)
    base._require_task_identity(task_id, "task ID")
    base._require_task_identity(task_revision, "task revision")
    if (task_id == source["task_id"] or task_revision == source["task_revision"]
            or reference_approval not in {"root_approved", "test_only"}):
        raise ValueError("A separate task identity and explicit reference approval are required")
    task = copy.deepcopy(source)
    task.update(task_id=task_id, task_revision=task_revision,
                title="Private endorsement review: independent Codex AI reference",
                banner="AI reviewed; human review pending", human_reviewed=False,
                review_status="pending_explicit_human_review")
    ledger = ["### Independent Codex AI reference",
              "All child rows are AI seeds. No row has human confirmation.",
              "accepted_by_ai_review means AI acceptance only. uncertain requires a human decision.",
              "Initial row numbers can change after edits.",
              "Each source uncertainty adds a separate pending block. Review every original failed model block separately.",
              "The app label failed model slots also includes Codex source uncertainties."]
    for parent, response in zip(task["parents"], reference["responses"], strict=True):
        parent["human_parent_confirmed"] = False
        parent["ai_parent_disposition"] = ("needs_review" if response["uncertainties"] else
                                            "contains_children" if response["children"] else "true_zero")
        parent["codex_ai_review_metadata"] = {k: copy.deepcopy(response[k]) for k in
            ("review_status", "reviewer_id", "reviewer_model", "complete_source_reviewed", "human_reviewed", "notes")}
        parent["ai_children"] = []
        uncertain = []
        for number, row in enumerate(response["children"], 1):
            seed = {k: copy.deepcopy(row[k]) for k in ("entity_type", "name_spans", "support_spans")}
            seed.update(human_confirmed=False, provisional_child_id=base.sha256_json({
                "reference_sha256": reference_sha256, "parent_document_id": parent["parent_document_id"],
                "row": number, "child": row}), ai_review_metadata={"ai_reviewed": True,
                    "human_reviewed": False, "endorsement_status": row["endorsement_status"]})
            parent["ai_children"].append(seed)
            if row["endorsement_status"] == "uncertain":
                uncertain.append(number)
        ledger.append(f"\n### Parent {response['parent_index']}: {parent['metadata']['candidate_icpsr']}")
        ledger.append("AI uncertain rows: " + (", ".join(map(str, uncertain)) or "none") + ".")
        for issue_number, issue in enumerate(response["uncertainties"], 1):
            spans = copy.deepcopy(issue["source_spans"])
            block_number = len(parent["ai_unresolved_blocks"]) + 1
            parent["ai_unresolved_blocks"].append({"unresolved_block_id": base.sha256_json({
                "reference_sha256": reference_sha256, "parent_document_id": parent["parent_document_id"],
                "issue": issue_number, "source_issue": issue}),
                "block_span": [spans[0][0], spans[-1][1]], "source_spans": spans,
                "reason": issue["reason"], "issue_kind": "independent_codex_source_uncertainty",
                "ai_reviewed": True, "human_reviewed": False})
            ledger.append(f"Block {block_number}, Codex issue {issue_number}, source spans {json.dumps(spans)}: {issue['reason']}")
    task["instructions"] += "\n\n" + "\n".join(ledger)
    task["contract"]["unresolved_block_rule"] = "every_original_failed_block_and_every_codex_source_issue_requires_separate_explicit_human_review"
    task["input_provenance"].update(source_task_sha256=SOURCE_TASK_SHA256,
        independent_ai_reference_sha256=reference_sha256, independent_source_manifest_sha256=source_manifest_sha256,
        reference_approval=reference_approval, original_model_child_count=2489,
        original_model_provisional_draft_sha256=source["input_provenance"]["provisional_draft_sha256"],
        provisional_draft_sha256=reference_sha256, ai_child_count=counts["accepted_children"] + counts["uncertain_children"],
        codex_source_issue_count=counts["uncertainty_blocks"], original_unresolved_block_count=11,
        ai_unresolved_block_count=11 + counts["uncertainty_blocks"],
        ai_true_zero_count=sum(p["ai_parent_disposition"] == "true_zero" for p in task["parents"]),
        ai_needs_review_count=sum(p["ai_parent_disposition"] == "needs_review" for p in task["parents"]))
    return task


def build_files(*, output_dir: Path, reference_path: Path, reference_sha256: str,
                source_manifest_sha256: str, task_id: str, task_revision: str,
                reference_approval: str, source_dir: Path = review.OUTPUT) -> dict:
    if output_dir.exists():
        raise ValueError("Output directory must not already exist")
    source, reference = load_inputs(reference_path=reference_path, reference_sha256=reference_sha256,
                                   source_manifest_sha256=source_manifest_sha256, source_dir=source_dir)
    task = build_task(source, reference, reference_sha256=reference_sha256,
        source_manifest_sha256=source_manifest_sha256, task_id=task_id, task_revision=task_revision,
        reference_approval=reference_approval)
    raw = base.canonical_bytes(task)
    task_hash = base.sha256_bytes(raw)
    html = base.render_html(task, task_hash).encode()
    auth = {"version": base.AUTH_VERSION, "builder_version": BUILDER_VERSION,
            "task_id": task_id, "task_revision": task_revision, "task_sha256": task_hash,
            "html_sha256": base.sha256_bytes(html), "source_task_sha256": SOURCE_TASK_SHA256,
            "independent_ai_reference_sha256": reference_sha256,
            "independent_source_manifest_sha256": source_manifest_sha256,
            "reference_approval": reference_approval, "generator_sha256": base.sha256_file(Path(__file__)),
            "renderer_sha256": base.sha256_file(Path(base.__file__)), "parent_count": 40,
            "ai_child_count": task["input_provenance"]["ai_child_count"],
            "unresolved_block_count": task["input_provenance"]["ai_unresolved_block_count"],
            "human_reviewed": False, "authorizations": copy.deepcopy(base.REVIEW_AUTHORIZATIONS)}
    output_dir.mkdir(parents=True, exist_ok=False)
    for name, data in [("task.json", raw), ("index.html", html), ("auth.json", base.canonical_bytes(auth))]:
        (output_dir / name).write_bytes(data)
        if len(data) < 100 or (output_dir / name).read_bytes() != data:
            raise ValueError(f"Built artifact differs: {name}")
    return auth


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--reference-path", type=Path, required=True)
    parser.add_argument("--reference-sha256", required=True)
    parser.add_argument("--source-manifest-sha256", required=True)
    parser.add_argument("--task-id", required=True)
    parser.add_argument("--task-revision", required=True)
    parser.add_argument("--reference-approval", choices=["root_approved", "test_only"], required=True)
    print(json.dumps(build_files(**vars(parser.parse_args())), indent=2))
