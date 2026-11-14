"""Build the private seven-item residual confirmation task for job 21576064."""
from __future__ import annotations

import argparse
import copy
import gzip
import hashlib
import json
import sys
from pathlib import Path
from typing import Any

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

from src.candidate_endorsement_line_ledger import (
    RECALL40_STAGE_B_RESIDUAL_PROJECTION_SPEC_SHA256,
    align_record_extraction_output,
    computed_recall40_stage_b_residual_contract,
    get_frozen_recall40_stage_b_contract,
    validate_record_extraction_output,
)
from scripts.hive.extract_candidate_endorsement_line_ledger_vllm import (
    validate_input_manifest,
)

EXPORT = REPO / "build/hive_candidate_extraction/v2_1_endorsement_40_parent_stage_b_preflight_20260827a"
OUTPUT = REPO / "build/hive_candidate_extraction/v2_1_endorsement_40_parent_stage_b_outputs_20260827b"
AUDIT = REPO / "quality_reports/candidate_endorsement_40_parent_stage_b_failed_21576064_2026-08-28.json"
AUDIT_SHA256 = "b69940304c1f70ef9028fd3be16542786ed7f7f74ee5f50ecceb07e784a6b93f"
OUTPUT_SHA256 = {
    "chunk_00000.out.jsonl.gz": "1aa103d0a0f4f65c56e7cc2fa74c179c075847be41ea27173de740d3922475c2",
    "chunk_00001.out.jsonl.gz": "dec4fee0dc11f735e2da92d9e50356c030f825b180359569668e3bacb92d81f1",
    "run_metadata_shard_00000.json": "e1a26d49be34dea2c3f51f43cb343f86978f55aad2abe0ab8be5fe5ec519a060",
    "run_metadata_shard_00001.json": "5f6e4a8f58e49eeceb58e123a0eb45323d83741f4cf006c8f3c224418649c6b0",
    "endorsement-line-ledger_21576064_0.log": "ee02ab79014bef5a378fcd3e071abe62d53067caca22e4e0695c121d62112639",
    "endorsement-line-ledger_21576064_1.log": "051ca37fc43588531921f2ff698a09b857cdc238ef5021516d42aba77b70ce0e",
}
HANDCODE_TEMPLATE = Path.home() / ".codex/skills/handcode/template.html"
HANDCODE_TEMPLATE_SHA256 = "ca1730c3352559151ae59eeaf8df8f1d57bf85c903ff13d794199c7b0304d33c"
FAILED_BATCH_IDS = (
    "7959d23cd32f101bfc2dd4ba0b0d183e201c300b21d129b62490acc9418a6731",
    "8840c0a453457b7a77807195c1e40c1bb126e74f3df374947f04ea44400a904a",
    "cc97761e4a09614b707bf2749e28af95566c8ec3530d3c57c99b8f7fca1a091b",
    "bbc8472d024a824981554118a3dfd9e9f73c9b47b9594f528ef8dea4c3b56799",
    "b2124cddaa9eae746d4965f18c20b450445b0ed896f9d9f6a4ff4895d52da8de",
    "4339d98ca308826ef22f211ebbc0e6447eb53a3606b94daf74c99499ea5f33e2",
    "2345780b3466e39d592b78b8c7217db4f225946bb997dce44033f3a767aeb824",
)


def sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def read_gzip(path: Path) -> list[dict[str, Any]]:
    with gzip.open(path, "rt", encoding="utf-8") as handle:
        return [json.loads(line) for line in handle]


def authenticated_inputs() -> tuple[dict[str, Any], dict[str, Any]]:
    validate_input_manifest(
        EXPORT / "manifest.json", EXPORT / "chunks",
        stage="record_extraction", contract=get_frozen_recall40_stage_b_contract(),
    )
    if sha256_file(AUDIT) != AUDIT_SHA256:
        raise ValueError("failed-run audit differs")
    if sorted(path.name for path in OUTPUT.iterdir()) != sorted(OUTPUT_SHA256):
        raise ValueError("failed-run six-file inventory differs")
    for name, expected in OUTPUT_SHA256.items():
        if sha256_file(OUTPUT / name) != expected:
            raise ValueError(f"failed-run artifact differs: {name}")
    batches = {
        row["batch_id"]: row
        for path in sorted((EXPORT / "chunks").glob("*.jsonl.gz"))
        for row in read_gzip(path)
    }
    rows = {
        row["batch_id"]: row
        for path in sorted(OUTPUT.glob("chunk_*.out.jsonl.gz"))
        for row in read_gzip(path)
    }
    if tuple(row for row in batches if rows[row]["technical_pass"] is False) != FAILED_BATCH_IDS:
        raise ValueError("failed batch order differs")
    return batches, rows


def proposed_output(batch_id: str, raw: dict[str, Any]) -> dict[str, Any]:
    proposal = copy.deepcopy(raw)
    if batch_id == FAILED_BATCH_IDS[1]:
        proposal["slot_05"]["records"][0].update({
            "endorser_name": "Ryan Zinke", "title": "Secretary",
        })
    elif batch_id == FAILED_BATCH_IDS[6]:
        proposal["slot_00"]["records"][0]["title"] = "Former Florida Congressman"
        proposal["slot_01"]["records"][0]["title"] = "Former Florida Congressman"
        proposal["slot_02"]["records"][0]["title"] = "Former California Congressman"
    return proposal


def build_task() -> dict[str, Any]:
    batches, rows = authenticated_inputs()
    contract = computed_recall40_stage_b_residual_contract()
    items = []
    for index, batch_id in enumerate(FAILED_BATCH_IDS, start=1):
        batch = batches[batch_id]
        raw = rows[batch_id]["model_output"]
        proposal = proposed_output(batch_id, raw)
        validation = validate_record_extraction_output(
            batch, proposal,
            residual_projection_spec_sha256=(
                RECALL40_STAGE_B_RESIDUAL_PROJECTION_SPEC_SHA256
            ),
        )
        alignment = align_record_extraction_output(
            batch, proposal,
            line_ledger_contract_sha256=contract["line_ledger_contract_sha256"],
            residual_projection_spec_sha256=(
                RECALL40_STAGE_B_RESIDUAL_PROJECTION_SPEC_SHA256
            ),
        )
        if not validation["schema_valid"] or not alignment["public_coverage_complete"]:
            raise ValueError(f"proposed tuple is not technically complete: {batch_id}")
        items.append({
            "id": batch_id,
            "label": f"Residual batch {index} of 7",
            "display": [
                {"label": "Authenticated source blocks", "format": "json", "content": [
                    {"slot": target["slot"], "line_text": target["line_text"],
                     "record_block_lines": target["record_block_lines"]}
                    for target in batch["targets"]
                ]},
                {"label": "Original model output", "format": "json", "content": raw},
                {"label": "Exact proposed six-field tuple", "format": "json", "content": proposal},
                {"label": "Technical proof", "format": "badges", "content": {
                    "Validation": "PASS", "Aligned records": alignment["aligned_record_count"],
                    "Withheld": alignment["withheld_record_count"],
                    "Proposal SHA": hashlib.sha256(json.dumps(
                        proposal, sort_keys=True, separators=(",", ":"), ensure_ascii=False
                    ).encode()).hexdigest(),
                }},
            ],
        })
    return {
        "task_id": "candidate_endorsement_recall40_stage_b_residual_21576064_6763970e",
        "title": "Recall40 Stage-B exact residual confirmation",
        "instructions": (
            "Review each authenticated source block and the complete proposed six-field "
            "output. Select **yes** only if every proposed record is the exact supported "
            "endorser representation. No field is editable. Select **no** if the sole "
            "proposed tuple is not substantively correct. Complete all seven items."
        ),
        "provenance": {
            "input_manifest_sha256": sha256_file(EXPORT / "manifest.json"),
            "failed_run_audit_sha256": AUDIT_SHA256,
            "failed_run_inventory_sha256": hashlib.sha256(json.dumps(
                OUTPUT_SHA256, sort_keys=True, separators=(",", ":")
            ).encode()).hexdigest(),
            "source_contract_sha256": get_frozen_recall40_stage_b_contract()[
                "line_ledger_contract_sha256"
            ],
            "residual_contract_sha256": contract["line_ledger_contract_sha256"],
            "projection_spec_sha256": RECALL40_STAGE_B_RESIDUAL_PROJECTION_SPEC_SHA256,
            "failed_batch_ids": list(FAILED_BATCH_IDS),
        },
        "items": items,
        "questions": [{
            "id": "approve_exact_tuple", "label": "Approve this exact complete tuple?",
            "type": "yesno", "required": True,
            "help": "Yes accepts the displayed tuple byte-semantically; no rejects it.",
        }],
    }


def render_html(task: dict[str, Any]) -> str:
    """Render and harden the exact complete-only residual app deterministically."""
    if sha256_file(HANDCODE_TEMPLATE) != HANDCODE_TEMPLATE_SHA256:
        raise ValueError("handcode template bytes differ")
    template = HANDCODE_TEMPLATE.read_text(encoding="utf-8")
    canonical_task = json.loads(json.dumps(task, ensure_ascii=False, sort_keys=True))
    payload = json.dumps(canonical_task, ensure_ascii=False).replace("</", "<\\/")
    html = template.replace("__TASK_DATA__", payload, 1)
    old = 'task_id: TASK.task_id, title: TASK.title || "", format: "handcode-results-v1",'
    new = 'task_id: TASK.task_id, format: "handcode-results-v1",'
    if html.count(old) != 1:
        raise ValueError("handcode result-shape seam differs")
    html = html.replace(old, new, 1)
    old = '''document.getElementById("btn-export").onclick = () => {
  const res = buildResults();
  document.getElementById("export-json").value = JSON.stringify(res, null, 2);
  document.getElementById("export-summary").textContent =
    res.n_complete + " of " + res.n_items + " items complete. Save as " + TASK.task_id + "_results.json" +
    (res.n_complete < res.n_items ? " — partial exports can be re-imported later." : ".");
  toggleOverlay("export-overlay", true);
};'''
    new = '''document.getElementById("btn-export").onclick = () => {
  const res = buildResults();
  if (res.n_complete !== res.n_items) {
    showBanner("Complete all 7 items before exporting.", "warn");
    return;
  }
  document.getElementById("export-json").value = JSON.stringify(res, null, 2);
  document.getElementById("export-summary").textContent =
    res.n_complete + " of " + res.n_items + " items complete. Save as " + TASK.task_id + "_results.json.";
  toggleOverlay("export-overlay", true);
};'''
    if html.count(old) != 1:
        raise ValueError("handcode complete-only export seam differs")
    return html.replace(old, new, 1)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-task", type=Path, required=True)
    parser.add_argument("--output-html", type=Path)
    args = parser.parse_args()
    task = build_task()
    args.output_task.parent.mkdir(parents=True, exist_ok=True)
    args.output_task.write_text(
        json.dumps(task, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n",
        encoding="utf-8",
    )
    if args.output_html:
        args.output_html.parent.mkdir(parents=True, exist_ok=True)
        args.output_html.write_text(render_html(task), encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
