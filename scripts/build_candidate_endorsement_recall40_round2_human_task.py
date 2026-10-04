#!/usr/bin/env python3
"""Build the private prediction-blind round-two recall coding app."""
from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path
from typing import Any, Mapping

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

from scripts.build_candidate_endorsement_line_ledger import (  # noqa: E402
    DEFAULT_FULL_EXPORT_DIR,
    reconstruct_selected_parents,
    replay_frozen_source,
)
from scripts.evaluate_candidate_endorsement_recall40_round2_stage_b import (  # noqa: E402
    INPUT_EXPORT,
    INPUT_MANIFEST_SHA256,
    evaluate,
)
from scripts.audit_candidate_endorsement_recall40_round2_stage_b import (  # noqa: E402
    build_audit,
)
from src.candidate_endorsement_line_ledger import sha256_json  # noqa: E402


TASK_ID = "candidate_endorsement_recall40_round2_human_truth_20260905"
TASK_VERSION = "candidate_endorsement_recall40_round2_human_truth_v2"


def sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def strict_json(path: Path) -> Any:
    def hook(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise ValueError(f"duplicate JSON key: {key}")
            result[key] = value
        return result

    return json.loads(
        path.read_text(encoding="utf-8"),
        object_pairs_hook=hook,
        parse_constant=lambda value: (_ for _ in ()).throw(
            ValueError(f"nonfinite JSON value: {value}")
        ),
    )


def build_task(output_dir: Path, authorization_report_path: Path) -> dict[str, Any]:
    observed = strict_json(authorization_report_path)
    if not isinstance(observed, Mapping):
        raise ValueError("round-two Stage-B authorization report is not an object")
    if observed.get("artifact_type") == (
        "immutable_recall40_round2_stage_b_technical_failure_audit"
    ):
        expected = build_audit(output_dir)
        authorization = observed.get("authorization")
        if (
            sha256_json(observed) != sha256_json(expected)
            or not isinstance(authorization, Mapping)
            or authorization.get("prediction_blind_human_truth_task_authorized")
            is not True
            or authorization.get("technical_failure_scoring_only") is not True
            or authorization.get("full_private_inference_authorized") is not False
            or authorization.get("release_authorized") is not False
        ):
            raise ValueError("round-two failed audit does not authorize the task")
        authorization_status = "authenticated_technical_failure_scoring_only"
        technical_pass_count = observed["authenticated_census"]["technical_pass"]
        output_artifacts_sha256 = sha256_json(observed["output_inventory"])
        aligned_records_sha256 = None
    else:
        expected = evaluate(output_dir)
        if sha256_json(observed) != sha256_json(expected):
            raise ValueError("round-two Stage-B gate report differs from exact replay")
        if (
            observed.get("gate_decision")
            != "GO_BUILD_PREDICTION_BLIND_HUMAN_TRUTH_TASK"
            or observed.get("prediction_blind_human_task_authorized") is not True
            or observed.get("full_private_inference_authorized") is not False
            or observed.get("release_authorized") is not False
        ):
            raise ValueError("round-two Stage-B gate does not authorize the task")
        authorization_status = "complete_technical_stage_b"
        technical_pass_count = observed["validated_batch_count"]
        output_artifacts_sha256 = observed["run_artifacts_sha256"]
        aligned_records_sha256 = observed["aligned_records_sha256"]
    input_manifest = strict_json(INPUT_EXPORT / "manifest.json")
    if sha256_file(INPUT_EXPORT / "manifest.json") != INPUT_MANIFEST_SHA256:
        raise ValueError("round-two Stage-B input manifest bytes differ")
    parent_ids = input_manifest.get("selected_parent_ids")
    if (
        not isinstance(parent_ids, list)
        or len(parent_ids) != 40
        or len(set(parent_ids)) != 40
    ):
        raise ValueError("round-two task parent selection differs")
    source = replay_frozen_source(
        DEFAULT_FULL_EXPORT_DIR, enforce_frozen_source=True
    )
    parents = reconstruct_selected_parents(
        source, parent_ids, round2_route_revision=True
    )
    if [parent["parent_document_id"] for parent in parents] != parent_ids:
        raise ValueError("round-two task parent order differs")
    task_parents = [
        {
            "parent_document_id": parent["parent_document_id"],
            "source_text_sha256": parent["extraction_source_text_sha256"],
            "source_n_chars": len(parent["parent_source_text"]),
            "parent_source_text": parent["parent_source_text"],
        }
        for parent in parents
    ]
    task = {
        "version": TASK_VERSION,
        "task_id": TASK_ID,
        "instructions": (
            "Code every distinct endorsement occurrence in the complete displayed "
            "parent. Select the exact endorser-name span and the exact text span "
            "that supplies endorsement support or context. Use true zero only when "
            "the complete displayed parent contains no endorsement occurrence. "
            "Do not consult model predictions."
        ),
        "stage_b_input_manifest_sha256": INPUT_MANIFEST_SHA256,
        "stage_b_authorization_report_sha256": sha256_file(
            authorization_report_path
        ),
        "stage_b_authorization_status": authorization_status,
        "stage_b_technical_pass_batch_count": technical_pass_count,
        "stage_b_output_artifacts_sha256": output_artifacts_sha256,
        "stage_b_record_ledger_sha256": aligned_records_sha256,
        "selected_parent_ids_sha256": input_manifest[
            "selected_parent_ids_sha256"
        ],
        "ordered_parent_ids_sha256": sha256_json(parent_ids),
        "parent_count": 40,
        "predictions_included": False,
        "parents": task_parents,
    }
    serialized = json.dumps(task, ensure_ascii=False, sort_keys=True)
    for forbidden in (
        "aligned_records", "endorser_category", "entity_type",
        "model_record_index", "model_output",
    ):
        if forbidden in serialized:
            raise ValueError(f"prediction-blind task leaked field: {forbidden}")
    return task


def render_html(task: Mapping[str, Any]) -> str:
    data = json.dumps(
        json.loads(json.dumps(task, ensure_ascii=False, sort_keys=True)),
        ensure_ascii=False,
    ).replace("</", "<\\/")
    return '''<!doctype html><html><head><meta charset="utf-8"><title>Round-two endorsement recall coding</title><style>
body{font:14px system-ui;margin:0;color:#17202a}header{position:sticky;top:0;background:#fff;padding:10px;border-bottom:1px solid #aaa;z-index:2}.src{height:50vh;overflow:auto;padding:10px;border:1px solid #888;background:#fff;font:13px ui-monospace,monospace;white-space:pre-wrap;user-select:text;cursor:text}.row{border:1px solid #ccc;padding:8px;margin:6px 0}button{margin:3px;padding:7px}.bad{color:#a00}::selection{background:#83bfff}main{padding:12px}</style></head><body>
<header><b id="pos"></b> <button id="prev">Prev</button><button id="next">Next</button><button id="zero">True zero</button><button id="name">Use selection as name</button><button id="support">Use selection as support</button><button id="add">Add occurrence</button><button id="export">Export complete results</button> <span id="msg"></span></header>
<main><details open><summary><b>Instructions</b></summary><p id="instructions"></p></details><pre id="src" class="src" tabindex="0"></pre><div id="draft"></div><div id="rows"></div></main>
<script id="task" type="application/json">'''+data+'''</script><script>
const T=JSON.parse(document.getElementById('task').textContent),key=T.task_id+':'+T.ordered_parent_ids_sha256+':'+T.stage_b_authorization_report_sha256,S=JSON.parse(localStorage.getItem(key)||'{}');let i=0,d={},last=null;const el=x=>document.getElementById(x),cur=()=>T.parents[i],save=()=>localStorage.setItem(key,JSON.stringify(S)),chars=s=>Array.from(s),slice=(s,a,b)=>chars(s).slice(a,b).join('');el('instructions').textContent=T.instructions;
function selectionSpan(){const root=el('src'),sel=window.getSelection();if(!sel||sel.rangeCount!==1||sel.isCollapsed)return null;const range=sel.getRangeAt(0);if(!root.contains(range.commonAncestorContainer))return null;const pre=document.createRange(),post=document.createRange();pre.selectNodeContents(root);pre.setEnd(range.startContainer,range.startOffset);post.selectNodeContents(root);post.setEnd(range.endContainer,range.endOffset);return[chars(pre.toString()).length,chars(post.toString()).length]}
function remember(){const span=selectionSpan();if(span&&span[0]<span[1]){last=span;el('msg').textContent='Selected: '+slice(cur().parent_source_text,span[0],span[1])}}
function take(){const span=selectionSpan()||last;if(!span||span[0]>=span[1])throw Error('Select a nonempty exact text span first');return span}
function esc(x){return x.replace(/[&<>]/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;'}[c]))}
function draw(){const p=cur(),r=S[p.parent_document_id]||{true_zero:false,occurrences:[]};el('src').textContent=p.parent_source_text;el('pos').textContent=`${i+1}/40 ${r.true_zero?'TRUE ZERO':r.occurrences.length+' occurrences'}`;el('rows').innerHTML=r.occurrences.map((o,j)=>`<div class=row><b>${j+1}</b> name ${o.name_start}:${o.name_end} “${esc(slice(p.parent_source_text,o.name_start,o.name_end))}”<br>support ${o.support_start}:${o.support_end} “${esc(slice(p.parent_source_text,o.support_start,o.support_end))}” <button onclick="drop(${j})">Delete</button></div>`).join('');d={};last=null;el('msg').textContent='';el('draft').textContent='Select the exact name and support spans.'}
window.drop=j=>{S[cur().parent_document_id].occurrences.splice(j,1);save();draw()};el('src').addEventListener('mouseup',remember);el('src').addEventListener('keyup',remember);document.addEventListener('selectionchange',()=>{if(document.activeElement===el('src'))remember()});for(const id of ['name','support'])el(id).addEventListener('mousedown',e=>e.preventDefault());
el('name').onclick=()=>{try{[d.name_start,d.name_end]=take();el('draft').textContent='Name: '+slice(cur().parent_source_text,d.name_start,d.name_end)}catch(e){el('msg').textContent=e.message}};el('support').onclick=()=>{try{[d.support_start,d.support_end]=take();el('draft').textContent+=' | Support: '+slice(cur().parent_source_text,d.support_start,d.support_end)}catch(e){el('msg').textContent=e.message}};
el('add').onclick=()=>{if(!['name_start','name_end','support_start','support_end'].every(k=>Number.isInteger(d[k])))return el('msg').textContent='Set both spans first';const id=cur().parent_document_id,r=S[id]||{true_zero:false,occurrences:[]};r.true_zero=false;r.occurrences.push({...d});S[id]=r;save();draw()};el('zero').onclick=()=>{S[cur().parent_document_id]={true_zero:true,occurrences:[]};save();draw()};el('prev').onclick=()=>{i=Math.max(0,i-1);draw()};el('next').onclick=()=>{i=Math.min(39,i+1);draw()};
el('export').onclick=()=>{const responses=T.parents.map(p=>({parent_document_id:p.parent_document_id,...(S[p.parent_document_id]||{})}));if(responses.some(r=>r.true_zero!==true&&!(Array.isArray(r.occurrences)&&r.occurrences.length)))return el('msg').textContent='Complete all 40 parents first';const out={task_id:T.task_id,format:'recall40-human-truth-v1',exported_at:new Date().toISOString(),parent_count:40,responses},a=document.createElement('a');a.href=URL.createObjectURL(new Blob([JSON.stringify(out,null,2)],{type:'application/json'}));a.download=T.task_id+'_results.json';a.click()};document.addEventListener('keydown',e=>{if(e.target===el('src'))return;if(e.key==='ArrowRight')el('next').click();if(e.key==='ArrowLeft')el('prev').click()});draw();
</script></body></html>'''


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--stage-b-output-dir", type=Path, required=True)
    parser.add_argument("--stage-b-authorization-report", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    if args.output_dir.exists():
        raise ValueError("output directory must not already exist")
    task = build_task(args.stage_b_output_dir, args.stage_b_authorization_report)
    args.output_dir.mkdir(parents=True)
    (args.output_dir / "task.json").write_text(
        json.dumps(task, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        + "\n",
        encoding="utf-8",
    )
    (args.output_dir / "index.html").write_text(
        render_html(task), encoding="utf-8"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
