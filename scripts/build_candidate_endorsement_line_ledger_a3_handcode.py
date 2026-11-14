#!/usr/bin/env python3
"""Build the authenticated residual Stage-A3 human adjudication task."""

from __future__ import annotations

import argparse
import copy
import hashlib
import json
import sys
from pathlib import Path
from typing import Any, Mapping

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.evaluate_candidate_endorsement_line_ledger_pilot import (
    evaluate_stage_a2_only,
    replay_stage_a2_merge,
)
from src.candidate_endorsement_line_ledger import ANCHOR_ROLES, sha256_json


A3_SPEC = {
    "spec_id": "candidate_endorsement_line_ledger_human_a3_v1",
    "derivation": (
        "exact union of invalid continuation-owner rows and interface-role "
        "mismatches in an authenticated Stage-A2-only evaluation"
    ),
    "allowed_final_roles": ["navigation_form_footer", "nonrecord_content"],
    "attachment_rule": (
        "attach only to an exact A2-allowed anchor whose final A2 role is an anchor"
    ),
    "coverage_rule": "exactly one typed human decision per derived residual row",
    "stage_b_rule": "forbidden until three-pass replay passes all unchanged gates",
}
A3_SPEC_SHA256 = sha256_json(A3_SPEC)
FROZEN_A3_TASK_SHA256 = (
    "e834f23d55804bc138d6dac1441dfbd96ab0449ddd34e1eeb0edf2585d96bfe5"
)
FROZEN_A3_HTML_SHA256 = (
    "ba9649cb0538ba6422fa6469f5455b38b0567ed988f47e5e060826a8659b73f3"
)
FROZEN_A3_RESULTS_SHA256 = (
    "f0a13ba66a617f46621d48109865d2ff88c6ba068a2e329d05093152958b74db"
)
FROZEN_A3_IMPORT_ARTIFACT_SHA256 = (
    "3178d0a55dadc2b3aaa3ac2916160e22e18211637d221a91d93b78981b8c528f"
)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _check_equal(observed: Any, expected: Any, label: str) -> None:
    if type(observed) is not type(expected) or observed != expected:
        raise ValueError(f"{label} differs")


def derive_a3_residuals(
    replayed: Mapping[str, Any], evaluation: Mapping[str, Any]
) -> list[dict[str, Any]]:
    """Derive residuals without embedding source-line IDs or expected labels."""
    merge = replayed["stage_a2_merge"]
    invalid_owner_ids = sorted({
        str(line_id)
        for report in merge["ownership_reports"]
        for line_id in report["invalid_continuation_owner_ids"]
    })
    terminal_mismatch_ids: set[str] = set()

    def collect_terminal_mismatches(expected: Any, observed: Any) -> None:
        if isinstance(expected, Mapping) and isinstance(observed, Mapping):
            for key in set(expected) & set(observed):
                expected_value = expected[key]
                observed_value = observed[key]
                if (
                    isinstance(key, str)
                    and len(key) == 64
                    and expected_value in A3_SPEC["allowed_final_roles"]
                    and observed_value in A3_SPEC["allowed_final_roles"]
                    and expected_value != observed_value
                ):
                    terminal_mismatch_ids.add(key)
                else:
                    collect_terminal_mismatches(expected_value, observed_value)

    for check in evaluation["checks"]:
        if check.get("passed") is False:
            collect_terminal_mismatches(check.get("expected"), check.get("observed"))
            observed = check.get("observed")
            if isinstance(observed, Mapping) and isinstance(
                observed.get("wrong_roles"), Mapping
            ):
                terminal_mismatch_ids.update(
                    str(line_id) for line_id in observed["wrong_roles"]
                )
    interface_ids = sorted(terminal_mismatch_ids)
    if not interface_ids:
        raise ValueError("Stage-A3 found no terminal-role mismatches")
    if set(invalid_owner_ids) & set(interface_ids):
        raise ValueError("Stage-A3 residual reasons overlap")

    parent_by_line: dict[str, tuple[Mapping[str, Any], Mapping[str, Any]]] = {}
    for parent in replayed["parents"]:
        for line in parent["lines"]:
            parent_by_line[str(line["source_line_id"])] = (parent, line)
    a2_target_by_id = {
        str(target["source_line_id"]): target
        for batch in replayed["stage_a2_batches"]
        for target in batch["targets"]
    }
    final = merge["final_decisions"]
    residuals = []
    for reason, line_ids in (
        ("invalid_continuation_owner", invalid_owner_ids),
        ("interface_role_mismatch", interface_ids),
    ):
        for line_id in line_ids:
            if line_id not in parent_by_line or line_id not in final:
                raise ValueError("Stage-A3 residual source identity is absent")
            parent, line = parent_by_line[line_id]
            target = a2_target_by_id.get(line_id)
            allowed = [] if target is None else [
                str(anchor_id)
                for anchor_id in target["allowed_anchor_source_line_ids"]
                if final.get(str(anchor_id), {}).get("role") in ANCHOR_ROLES
            ]
            allowed_details = [{
                "source_line_id": anchor_id,
                "parent_line_index": int(parent_by_line[anchor_id][1][
                    "parent_line_index"
                ]),
                "line_text": str(parent_by_line[anchor_id][1]["line_text"]),
            } for anchor_id in allowed]
            index = int(line["parent_line_index"])
            lines = list(parent["lines"])
            context = lines[max(0, index - 3):min(len(lines), index + 4)]
            residuals.append({
                "source_line_id": line_id,
                "parent_document_id": str(parent["parent_document_id"]),
                "parent_line_index": index,
                "line_text": str(line["line_text"]),
                "line_text_sha256": str(line["line_text_sha256"]),
                "residual_reason": reason,
                "stage_a2_final_role": str(final[line_id]["role"]),
                "stage_a2_continuation_owner_source_line_id": final[line_id].get(
                    "continuation_owner_source_line_id"
                ),
                "eligible_final_anchor_source_line_ids": allowed,
                "eligible_final_anchors": allowed_details,
                "context_lines": [{
                    "source_line_id": str(value["source_line_id"]),
                    "parent_line_index": int(value["parent_line_index"]),
                    "line_text": str(value["line_text"]),
                } for value in context],
            })
    residuals.sort(key=lambda row: (
        row["parent_document_id"], row["parent_line_index"], row["source_line_id"]
    ))
    if len({row["source_line_id"] for row in residuals}) != len(residuals):
        raise ValueError("Stage-A3 residual identities are duplicated")
    return residuals


def build_a3_provenance(
    replayed: Mapping[str, Any],
    residuals: list[dict[str, Any]],
    *,
    stage_a2_export_dir: Path,
    stage_a2_evaluation_report: Path,
) -> dict[str, Any]:
    return {
        "a3_spec": A3_SPEC,
        "a3_spec_sha256": A3_SPEC_SHA256,
        "stage_a_contract_sha256": replayed["stage_a_manifest"][
            "line_ledger_contract_sha256"
        ],
        "stage_a2_contract_sha256": replayed["stage_a2_manifest"][
            "line_ledger_contract_sha256"
        ],
        "stage_a2_input_manifest_sha256": sha256_file(
            stage_a2_export_dir / "manifest.json"
        ),
        "stage_a2_output_artifacts": copy.deepcopy(
            replayed["stage_a2_artifacts"]
        ),
        "stage_a2_evaluation_report_sha256": sha256_file(
            stage_a2_evaluation_report
        ),
        "stage_a2_final_decisions_sha256": replayed["stage_a2_merge"][
            "final_decisions_sha256"
        ],
        "stage_a2_per_line_provenance_sha256": replayed["stage_a2_merge"][
            "per_line_provenance_sha256"
        ],
        "ordered_residual_core_sha256": sha256_json([{
            key: row[key] for key in (
                "source_line_id", "parent_document_id", "parent_line_index",
                "line_text_sha256", "residual_reason", "stage_a2_final_role",
                "stage_a2_continuation_owner_source_line_id",
                "eligible_final_anchor_source_line_ids",
            )
        } for row in residuals]),
        "residual_count": len(residuals),
    }


def build_task(args: argparse.Namespace) -> dict[str, Any]:
    replayed = replay_stage_a2_merge(
        args.stage_a_export_dir,
        args.stage_a_output_dir,
        args.stage_a2_export_dir,
        args.stage_a2_output_dir,
        args.full_export_dir,
    )
    recomputed = evaluate_stage_a2_only(
        args.stage_a_export_dir,
        args.stage_a_output_dir,
        args.stage_a2_export_dir,
        args.stage_a2_output_dir,
        args.full_export_dir,
    )
    stored = json.loads(args.stage_a2_evaluation_report.read_text())
    _check_equal(stored, recomputed, "Stage-A2 evaluation report replay")
    residuals = derive_a3_residuals(replayed, recomputed)
    provenance = build_a3_provenance(
        replayed,
        residuals,
        stage_a2_export_dir=args.stage_a2_export_dir,
        stage_a2_evaluation_report=args.stage_a2_evaluation_report,
    )
    task = {
        "task_id": "candidate_endorsement_line_ledger_human_a3_21447139",
        "title": "Endorsement line-ledger residual adjudication",
        "instructions": (
            "### Task\nClassify each highlighted source line using only its text and "
            "nearby page lines.\n\n- **navigation_form_footer**: campaign contact, "
            "address, form, navigation, disclaimer, or footer material.\n- "
            "**nonrecord_content**: other page content that is not an endorsement "
            "record or continuation.\n- **attach**: use only when the target truly "
            "continues one of the explicitly listed eligible anchors.\n\nDo not infer "
            "an endorsement from contact information. Every row requires one decision."
        ),
        "provenance": provenance,
        "items": [{
            "id": row["source_line_id"],
            "label": f"{row['residual_reason']} · line {row['parent_line_index']}",
            "residual": row,
            "display": [
                {"label": "Binding", "format": "badges", "content": {
                    "Reason": row["residual_reason"],
                    "Current role": row["stage_a2_final_role"],
                    "Parent": row["parent_document_id"][:12],
                    "Line": row["parent_line_index"],
                }},
                {"label": "Target source line", "format": "text",
                 "content": row["line_text"], "highlight": [row["line_text"]]},
                {"label": "Nearby source lines", "format": "text",
                 "content": "\n".join(
                     f"{value['parent_line_index']}: {value['line_text']}"
                     for value in row["context_lines"]
                 ), "highlight": [
                     f"{row['parent_line_index']}: {row['line_text']}"
                 ]},
                {"label": "Eligible final anchors", "format": "json",
                 "content": row["eligible_final_anchors"]},
            ],
        } for row in residuals],
        "questions": [{
            "id": "resolution",
            "label": "Resolution",
            "type": "select",
            "options": ["terminal", "attach"],
            "required": True,
        }, {
            "id": "final_role",
            "label": "Terminal structural role",
            "type": "select",
            "options": list(A3_SPEC["allowed_final_roles"]),
            "required": True,
            "show_if": {"question": "resolution", "equals": "terminal"},
        }, {
            "id": "anchor_source_line_id",
            "label": "Eligible anchor source-line ID",
            "type": "select",
            "options": sorted({
                anchor_id for row in residuals
                for anchor_id in row["eligible_final_anchor_source_line_ids"]
            }),
            "required": True,
            "show_if": {"question": "resolution", "equals": "attach"},
        }],
    }
    args.output_task.parent.mkdir(parents=True, exist_ok=False)
    args.output_task.write_text(
        json.dumps(task, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    )
    return task


def build_exact_html(task: Mapping[str, Any], output_html: Path) -> None:
    """Build a small app whose choices are exact for each individual row."""
    payload = json.dumps(task, ensure_ascii=False).replace("</", "<\\/")
    html = """<!doctype html><meta charset="utf-8"><title>A3 adjudication</title>
<style>body{font:16px system-ui;margin:0;background:#f5f3ee;color:#17202a}main{max-width:980px;margin:auto;padding:24px}button{padding:9px 12px;margin:4px;border:1px solid #555;border-radius:6px;background:white}button.sel{background:#17324d;color:white}.line{white-space:pre-wrap;background:white;padding:14px;border-radius:6px}.target{background:#fff1a8}.muted{color:#59636e}.bar{position:sticky;top:0;background:#f5f3ee;padding:8px 0}code{font-size:12px}</style>
<main><div class="bar"><b id="progress"></b><button onclick="move(-1)">← Previous</button><button onclick="move(1)">Next →</button><button onclick="exportResults()">Export JSON</button></div><h2 id="label"></h2><div id="meta"></div><h3>Nearby source lines</h3><div class="line" id="context"></div><h3>Decision</h3><div id="choices"></div><p class="muted">Keys: n = navigation/footer, c = other nonrecord, 1–9 = listed eligible anchor, ←/→ = move.</p></main>
<script>const task=__TASK__;let i=0;const key='a3:'+task.task_id;let answers=JSON.parse(localStorage.getItem(key)||'{}');
function options(item){let out=[['n','terminal','navigation_form_footer','Navigation / form / footer'],['c','terminal','nonrecord_content','Other nonrecord content']];item.residual.eligible_final_anchors.forEach((a,j)=>out.push([String(j+1),'attach',a.source_line_id,'Attach to '+a.parent_line_index+': '+a.line_text]));return out}
function render(){const item=task.items[i],r=item.residual,a=answers[item.id];progress.textContent=`${i+1}/${task.items.length} · ${Object.keys(answers).length} coded`;label.textContent=item.label;meta.innerHTML=`<code>${item.id}</code><br>Reason: ${r.residual_reason}<br>Current A2 role: ${r.stage_a2_final_role}`;context.innerHTML=r.context_lines.map(x=>`<div class="${x.source_line_id===item.id?'target':''}">${x.parent_line_index}: ${esc(x.line_text)}</div>`).join('');choices.innerHTML='';options(item).forEach(o=>{const b=document.createElement('button');b.textContent=`${o[0]} · ${o[3]}`;if(a&&a.resolution===o[1]&&(a.final_role===o[2]||a.anchor_source_line_id===o[2]))b.className='sel';b.onclick=()=>choose(o);choices.appendChild(b)})}
function esc(s){return s.replace(/[&<>]/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;'}[c]))}function choose(o){answers[task.items[i].id]=o[1]==='attach'?{resolution:'attach',anchor_source_line_id:o[2]}:{resolution:'terminal',final_role:o[2]};localStorage.setItem(key,JSON.stringify(answers));render();if(i<task.items.length-1)setTimeout(()=>move(1),120)}function move(d){i=Math.max(0,Math.min(task.items.length-1,i+d));render()}
function exportResults(){const responses=task.items.map(x=>({item_id:x.id,status:answers[x.id]?'complete':'incomplete',answers:answers[x.id]||{}}));const complete=responses.filter(x=>x.status==='complete').length;if(complete!==task.items.length){alert(`Complete all ${task.items.length} rows first.`);return}const out={task_id:task.task_id,format:'handcode-results-v1',exported_at:new Date().toISOString(),n_items:task.items.length,n_complete:complete,responses};const blob=new Blob([JSON.stringify(out,null,2)+'\\n'],{type:'application/json'}),a=document.createElement('a');a.href=URL.createObjectURL(blob);a.download=task.task_id+'_results.json';a.click();URL.revokeObjectURL(a.href)}
document.addEventListener('keydown',e=>{if(e.key==='ArrowLeft')move(-1);else if(e.key==='ArrowRight')move(1);else{const o=options(task.items[i]).find(x=>x[0]===e.key.toLowerCase());if(o)choose(o)}});render();</script>""".replace("__TASK__", payload)
    output_html.write_text(html)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--stage-a-export-dir", type=Path, required=True)
    parser.add_argument("--stage-a-output-dir", type=Path, required=True)
    parser.add_argument("--stage-a2-export-dir", type=Path, required=True)
    parser.add_argument("--stage-a2-output-dir", type=Path, required=True)
    parser.add_argument("--stage-a2-evaluation-report", type=Path, required=True)
    parser.add_argument("--full-export-dir", type=Path, required=True)
    parser.add_argument("--output-task", type=Path, required=True)
    parser.add_argument("--output-html", type=Path)
    args = parser.parse_args()
    task = build_task(args)
    if args.output_html is not None:
        build_exact_html(task, args.output_html)
    print(json.dumps({
        "output_task": str(args.output_task),
        "items": len(task["items"]),
        "task_sha256": sha256_file(args.output_task),
        "output_html": str(args.output_html) if args.output_html else None,
        "html_sha256": (
            sha256_file(args.output_html) if args.output_html else None
        ),
        "provenance": task["provenance"],
    }, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
