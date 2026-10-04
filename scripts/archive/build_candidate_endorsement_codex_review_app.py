#!/usr/bin/env python3
"""Build a separate human task from the pinned Codex review."""
from __future__ import annotations

import argparse
import copy
import json
import sys
from collections import Counter
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))
from scripts import build_candidate_endorsement_round4_human_review_app as base
from scripts.review_candidate_endorsement_codex import validate_review

SOURCE_TASK = REPO / "build/private_candidate_endorsement_recall40_round4_human_review_20260907b/task.json"
SOURCE_TASK_SHA256 = "c92cb24a4c692f839f009b40f4be898e729db669da4c2e11e25fdec53fd195ea"
AI_REVIEW = REPO / "build/private_candidate_endorsement_codex_ai_review_20260914a/ai_reviewed_results_v2.json"
AI_REVIEW_SHA256 = "56abd8a8f34f2ab3f13498b838792f714784430747a7405d08810e6ca4887704"
OUTPUT = REPO / "build/private_candidate_endorsement_codex_human_review_20260914a"
TASK_ID = "candidate_endorsement_codex_human_review_20260914a"
TASK_REVISION = "candidate_endorsement_codex_human_review_v2_20260914a"
BUILDER_VERSION = "candidate_endorsement_codex_human_review_adapter_v1"


def _pinned_json(path: Path, expected: str) -> dict:
    raw = path.read_bytes()
    if base.sha256_bytes(raw) != expected:
        raise ValueError(f"Pinned input hash differs: {path}")
    return base.strict_json_bytes(raw, label=str(path))


def validate_inputs(task: dict, reference: dict) -> None:
    if (task["parent_count"] != 40 or len(task["parents"]) != 40
            or reference["parent_count"] != 40 or len(reference["responses"]) != 40
            or reference["source_task_sha256"] != SOURCE_TASK_SHA256
            or reference["review_status"] != "ai_reviewed"
            or reference["human_reviewed"] is not False
            or task["authorizations"] != base.REVIEW_AUTHORIZATIONS
            or any(value is not False for value in reference["authorizations"].values())):
        raise ValueError("Input identity, census, or authority differs")
    counts = Counter()
    ids = set()
    for index, (parent, reviewed) in enumerate(zip(task["parents"], reference["responses"], strict=True), 1):
        source = parent["parent_source_text"]
        if (parent["parent_document_id"] in ids
                or parent["source_n_chars"] != len(source)
                or base.sha256_bytes(source.encode()) != parent["source_text_sha256"]
                or parent["human_parent_confirmed"] is not False
                or reviewed["parent_index"] != index
                or reviewed["metadata"] != parent["metadata"]
                or reviewed["complete_source_ai_reviewed"] is not True
                or reviewed["parent_disposition"] not in {"contains_children", "true_zero", "unclear"}):
            raise ValueError(f"Parent source or authority differs: {index}")
        ids.add(parent["parent_document_id"])
        validate_review({**reviewed, "review_status": "ai_reviewed",
                         "complete_source_reviewed": reviewed["complete_source_ai_reviewed"],
                         "reviewer_id": ",".join(reviewed["reviewer_ids"])},
                        {**parent, "parent_index": index, "source_text": source})
        for child in reviewed["children"]:
            if (child["ai_reviewed"] is not True or child["human_reviewed"] is not False
                    or child["endorsement_status"] not in {"accepted_by_ai_review", "uncertain"}):
                raise ValueError("AI child authority or status differs")
            counts[child["endorsement_status"]] += 1
        for child in parent["ai_children"]:
            if child["human_confirmed"] is not False:
                raise ValueError("Original AI child has human confirmation")
        for block in parent["ai_unresolved_blocks"]:
            if block["human_reviewed"] is not False:
                raise ValueError("Original block has human confirmation")
            base._validate_spans([block["block_span"]], source, label="original block", allow_empty=False)
        counts["uncertainties"] += len(reviewed["uncertainties"])
        counts["original_blocks"] += len(parent["ai_unresolved_blocks"])
    if (counts != Counter({"accepted_by_ai_review": 1093, "uncertain": 867,
                           "uncertainties": 71, "original_blocks": 17})
            or reference["counts"]["reviewed_children"] != 1960
            or reference["counts"]["uncertainties"] != 71):
        raise ValueError("AI seed or unresolved issue counts differ")


def load_inputs(source_path: Path = SOURCE_TASK, review_path: Path = AI_REVIEW) -> tuple[dict, dict]:
    task = _pinned_json(source_path, SOURCE_TASK_SHA256)
    reference = _pinned_json(review_path, AI_REVIEW_SHA256)
    for entry in reference["review_file_inventory"]:
        path = (REPO / entry["path"]).resolve()
        if not path.is_relative_to(REPO) or base.sha256_file(path) != entry["sha256"]:
            raise ValueError("Pinned review inventory differs")
    validate_inputs(task, reference)
    return task, reference


def build_task(source_task: dict, reference: dict) -> dict:
    validate_inputs(source_task, reference)
    task = copy.deepcopy(source_task)
    task.update(task_id=TASK_ID, task_revision=TASK_REVISION,
                title="Private endorsement review: Codex AI review v2",
                banner="AI reviewed; human review pending", human_reviewed=False,
                review_status="pending_explicit_human_review")
    ledger = ["### Codex seed status and source issues",
              "All 1,960 rows are AI seeds. No row has human confirmation.",
              "The status accepted_by_ai_review means AI acceptance only. The status uncertain requires a human decision.",
              "The row numbers below refer to the initial seeds. Edits can change the row numbers.",
              "Each uncertainty adds one pending block. The block can cover several exact source spans.",
              "The app reuses the label failed model slots for all pending blocks. This label also includes Codex source issues.",
              "Inspect each issue below. Correct the children, then mark its matching block explicitly reviewed.",
              "The 17 original failed blocks also require separate review."]
    for parent, reviewed in zip(task["parents"], reference["responses"], strict=True):
        parent["human_parent_confirmed"] = False
        parent["ai_parent_disposition"] = {"unclear": "needs_review"}.get(reviewed["parent_disposition"], reviewed["parent_disposition"])
        parent["codex_ai_review_metadata"] = {key: copy.deepcopy(reviewed[key]) for key in
                                             ("parent_disposition", "reviewer_ids", "adjudication_notes", "complete_source_ai_reviewed", "human_reviewed")}
        parent["ai_children"] = []
        uncertain_rows = []
        for number, row in enumerate(reviewed["children"], 1):
            seed = {key: copy.deepcopy(row[key]) for key in ("entity_type", "name_spans", "support_spans")}
            seed.update(human_confirmed=False, provisional_child_id=base.sha256_json({
                "ai_review_sha256": AI_REVIEW_SHA256, "parent_document_id": parent["parent_document_id"], "row": number, "child": row}),
                ai_review_metadata={key: row[key] for key in ("ai_reviewed", "human_reviewed", "endorsement_status")})
            parent["ai_children"].append(seed)
            if row["endorsement_status"] == "uncertain":
                uncertain_rows.append(number)
        ledger.append(f"\n### Parent {reviewed['parent_index']}: {parent['metadata']['candidate_icpsr']}")
        ledger.append(f"AI parent status: {reviewed['parent_disposition']}. Human parent status: unconfirmed.")
        ledger.append("AI uncertain rows: " + (", ".join(map(str, uncertain_rows)) or "none") + ". All other initial rows have status accepted_by_ai_review.")
        for issue_number, issue in enumerate(reviewed["uncertainties"], 1):
            spans = copy.deepcopy(issue["source_spans"])
            block_id = base.sha256_json({"ai_review_sha256": AI_REVIEW_SHA256,
                                        "parent_document_id": parent["parent_document_id"], "issue": issue_number, "source_issue": issue})
            block_number = len(parent["ai_unresolved_blocks"]) + 1
            parent["ai_unresolved_blocks"].append({"unresolved_block_id": block_id,
                "block_span": [spans[0][0], spans[-1][1]], "source_spans": spans,
                "reason": issue["reason"], "issue_kind": "codex_source_uncertainty",
                "ai_reviewed": True, "human_reviewed": False})
            ledger.append(f"Block {block_number}, Codex issue {issue_number}, source spans {json.dumps(spans)}: {issue['reason']}")
    task["instructions"] += "\n\n" + "\n".join(ledger)
    task["contract"]["unresolved_block_rule"] = "every_original_failed_block_and_every_codex_source_issue_requires_separate_explicit_human_review"
    task["input_provenance"].update(source_task_sha256=SOURCE_TASK_SHA256,
        codex_ai_review_sha256=AI_REVIEW_SHA256, provisional_draft_sha256=AI_REVIEW_SHA256,
        ai_child_count=1960, ai_unresolved_block_count=88, codex_source_issue_count=71,
        original_unresolved_block_count=17,
        ai_true_zero_count=sum(p["ai_parent_disposition"] == "true_zero" for p in task["parents"]),
        ai_needs_review_count=sum(p["ai_parent_disposition"] == "needs_review" for p in task["parents"]))
    return task


def build_files(output_dir: Path = OUTPUT) -> dict:
    if output_dir.exists():
        raise ValueError("Output directory must not already exist")
    source, reference = load_inputs()
    task = build_task(source, reference)
    raw = base.canonical_bytes(task)
    task_hash = base.sha256_bytes(raw)
    html = base.render_html(task, task_hash).encode()
    auth = {"version": base.AUTH_VERSION, "builder_version": BUILDER_VERSION,
            "task_id": TASK_ID, "task_revision": TASK_REVISION, "task_sha256": task_hash,
            "html_sha256": base.sha256_bytes(html), "source_task_sha256": SOURCE_TASK_SHA256,
            "codex_ai_review_sha256": AI_REVIEW_SHA256, "generator_sha256": base.sha256_file(Path(__file__)),
            "renderer_sha256": base.sha256_file(Path(base.__file__)),
            "parent_count": 40, "ai_child_count": 1960, "codex_source_issue_count": 71,
            "original_unresolved_block_count": 17, "unresolved_block_count": 88,
            "human_reviewed": False, "authorizations": copy.deepcopy(base.REVIEW_AUTHORIZATIONS)}
    output_dir.mkdir(parents=True, exist_ok=False)
    for name, data in [("task.json", raw), ("index.html", html), ("auth.json", base.canonical_bytes(auth))]:
        (output_dir / name).write_bytes(data)
        if len(data) < 100 or (output_dir / name).read_bytes() != data:
            raise ValueError(f"Built artifact differs: {name}")
    return auth


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, default=OUTPUT)
    print(json.dumps(build_files(parser.parse_args().output_dir), indent=2))
