"""Build the private, prediction-blind complete-parent recall coding task."""
from __future__ import annotations

import argparse, hashlib, json, sys
from collections import defaultdict
from pathlib import Path
from typing import Any

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path: sys.path.insert(0, str(REPO))

from scripts.audit_candidate_endorsement_40_parent_feasibility import authenticated_full_rows
from src.candidate_endorsement_line_ledger import reconstruct_parent_inventory, sha256_json

SELECTION = REPO / "build/private_candidate_endorsement_40_parent_selection_20260827/pilot40_selection.json"
SELECTION_SHA = "5e6b0908f12cd59560699079fe19ea190303b0a8a14b9cebc1792761ed0567e6"
SPEC = REPO / "quality_reports/candidate_endorsement_40_parent_recall_pilot_selection_spec_2026-08-26.json"
SPEC_SHA = "47303d1cf0d075fe105011d48cf4329dc41d4f2842087dae7b80aad68eea1f8f"
RESIDUAL_DIR = REPO / "build/candidate_endorsement_recall40_stage_b_residual_21576064"
RESIDUAL_SHAS = {"results.json":"12611bcaba1cedb566d431c91f6d380b237d677255381fc28e9c1db10c29660e",
 "import_artifact.json":"6b1fcc53dd87cde59396ea78229fa95e2ea123e96656f6ab0661e0de714e418a",
 "evaluation.json":"00e12ec019cd1b7ae66882d6d4fecac6ba66f0c8b2143b51728f8a4b9fe45027"}

def sha(path: Path) -> str: return hashlib.sha256(path.read_bytes()).hexdigest()

def authenticated_parents() -> list[dict[str, Any]]:
    if sha(SELECTION) != SELECTION_SHA or sha(SPEC) != SPEC_SHA: raise ValueError("selection/spec bytes differ")
    for name, digest in RESIDUAL_SHAS.items():
        if sha(RESIDUAL_DIR/name) != digest: raise ValueError(f"residual artifact differs: {name}")
    selection=json.loads(SELECTION.read_text())
    ids=[p for c in selection["pilot_components"] for p in c["parent_document_ids"]]
    if len(ids)!=40 or len(set(ids))!=40: raise ValueError("pilot parent closure differs")
    rows,_=authenticated_full_rows(); by=defaultdict(list)
    for row in rows:
        if row["route"]=="endorsements" and row["parent_document_id"] in set(ids): by[row["parent_document_id"]].append(row)
    out=[]
    for pid in ids:
        parent=reconstruct_parent_inventory(by[pid])
        out.append({"parent_document_id":pid,"source_text_sha256":parent["extraction_source_text_sha256"],
                    "parent_source_text":parent["parent_source_text"],"source_n_chars":len(parent["parent_source_text"])})
    return out

def build_task() -> dict[str, Any]:
    parents=authenticated_parents()
    return {"version":"candidate_endorsement_recall40_human_truth_task_v2_source_spans",
      "task_id":"candidate_endorsement_recall40_human_truth_20260828_source_spans_v2",
      "instructions":"Code every distinct endorsement occurrence in the complete parent. Select the exact endorser-name span and the exact support/context span. Use true zero only when the complete parent contains no endorsement occurrence. Do not consult model predictions.",
      "selection_spec_sha256":SPEC_SHA,"private_selection_sha256":SELECTION_SHA,
      "residual_results_sha256":RESIDUAL_SHAS["results.json"],"residual_import_sha256":RESIDUAL_SHAS["import_artifact.json"],
      "residual_evaluation_sha256":RESIDUAL_SHAS["evaluation.json"],
      "support_locator_spec_sha256":"47303d1cf0d075fe105011d48cf4329dc41d4f2842087dae7b80aad68eea1f8f","parent_count":40,
      "ordered_parent_ids_sha256":sha256_json([p["parent_document_id"] for p in parents]),"parents":parents}

def render(task: dict[str, Any]) -> str:
    data=json.dumps(json.loads(json.dumps(task,ensure_ascii=False,sort_keys=True)),ensure_ascii=False).replace("</","<\\/")
    return '''<!doctype html><meta charset="utf-8"><title>Recall40 human truth</title><style>body{font:14px system-ui;margin:0}header{position:sticky;top:0;background:#fff;padding:10px;border-bottom:1px solid #bbb;z-index:1}main{padding:12px}.src{display:block;width:auto;height:48vh;overflow:auto;padding:8px;border:1px solid #999;background:#fff;font:13px ui-monospace;white-space:pre-wrap;user-select:text;cursor:text}.row{border:1px solid #ccc;padding:8px;margin:6px 0}button{margin:4px;padding:7px}.bad{color:#a00}::selection{background:#9dc7ff}</style><header><b id="pos"></b> <button id="prev">Prev</button><button id="next">Next</button><button id="zero">True zero</button><button id="name">Set name span</button><button id="support">Set support span</button><button id="add">Add occurrence</button><button id="export">Export complete results</button><span id="msg"></span></header><main><details open><summary><b>Instructions</b></summary><p id="instructions"></p></details><pre id="src" class="src" tabindex="0"></pre><div id="draft"></div><div id="rows"></div></main><script id="task" type="application/json">'''+data+'''</script><script>
const T=JSON.parse(document.getElementById('task').textContent), key=T.task_id+':'+T.ordered_parent_ids_sha256+':'+T.residual_evaluation_sha256+':'+T.support_locator_spec_sha256, S=JSON.parse(localStorage.getItem(key)||'{}');let i=0,d={},lastSpan=null;document.getElementById('instructions').textContent=T.instructions;
const el=x=>document.getElementById(x), cur=()=>T.parents[i], save=()=>localStorage.setItem(key,JSON.stringify(S)), cp=(s,n)=>Array.from(s.slice(0,n)).length, cpslice=(s,a,b)=>Array.from(s).slice(a,b).join('');
function selectedSpan(){let root=el('src'),s=window.getSelection();if(!s||s.rangeCount!==1||s.isCollapsed)return null;let r=s.getRangeAt(0);if(!root.contains(r.commonAncestorContainer))return null;let a=document.createRange(),b=document.createRange();a.selectNodeContents(root);a.setEnd(r.startContainer,r.startOffset);b.selectNodeContents(root);b.setEnd(r.endContainer,r.endOffset);let text=root.textContent;return[cp(text,a.toString().length),cp(text,b.toString().length)]}
function rememberSpan(){let x=selectedSpan();if(x&&x[0]<x[1]){lastSpan=x;el('msg').textContent='Selected: '+cpslice(cur().parent_source_text,x[0],x[1])}}
function span(){let x=selectedSpan()||lastSpan;if(!x||!Number.isInteger(x[0])||x[0]>=x[1])throw Error('Select a nonempty exact span first');return x}
function draw(){let p=cur(),r=S[p.parent_document_id]||{true_zero:false,occurrences:[]};el('src').textContent=p.parent_source_text;el('pos').textContent=`${i+1}/40 ${p.parent_document_id.slice(0,10)} ${r.true_zero?'TRUE ZERO':r.occurrences.length+' occurrences'}`;el('rows').innerHTML=r.occurrences.map((o,j)=>`<div class=row><b>${j+1}</b> name ${o.name_start}:${o.name_end} “${esc(cpslice(p.parent_source_text,o.name_start,o.name_end))}”<br>support ${o.support_start}:${o.support_end} “${esc(cpslice(p.parent_source_text,o.support_start,o.support_end))}” <button onclick="drop(${j})">Delete</button></div>`).join('');d={};lastSpan=null;el('msg').textContent='';el('draft').textContent='Select exact name and support/context spans.'}
function esc(x){return x.replace(/[&<>]/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;'}[c]))}function drop(j){S[cur().parent_document_id].occurrences.splice(j,1);save();draw()}
el('name').onclick=()=>{try{[d.name_start,d.name_end]=span();el('draft').textContent='Name: '+cpslice(cur().parent_source_text,d.name_start,d.name_end)}catch(e){el('msg').textContent=e.message}};
el('support').onclick=()=>{try{[d.support_start,d.support_end]=span();el('draft').textContent+=' | Support: '+cpslice(cur().parent_source_text,d.support_start,d.support_end)}catch(e){el('msg').textContent=e.message}};
el('src').addEventListener('mouseup',rememberSpan);el('src').addEventListener('keyup',rememberSpan);document.addEventListener('selectionchange',()=>{if(document.activeElement===el('src'))rememberSpan()});
for(const id of ['name','support'])el(id).addEventListener('mousedown',e=>e.preventDefault());
el('add').onclick=()=>{if(!['name_start','name_end','support_start','support_end'].every(k=>Number.isInteger(d[k])))return el('msg').textContent='Set both spans first';let id=cur().parent_document_id,r=S[id]||{true_zero:false,occurrences:[]};r.true_zero=false;r.occurrences.push(d);S[id]=r;save();draw()};
el('zero').onclick=()=>{S[cur().parent_document_id]={true_zero:true,occurrences:[]};save();draw()};el('prev').onclick=()=>{i=Math.max(0,i-1);draw()};el('next').onclick=()=>{i=Math.min(39,i+1);draw()};
el('export').onclick=()=>{let responses=T.parents.map(p=>({parent_document_id:p.parent_document_id,...(S[p.parent_document_id]||{})}));if(responses.some(r=>r.true_zero!==true&&!(Array.isArray(r.occurrences)&&r.occurrences.length)))return el('msg').textContent='Complete all 40 parents first';let out={task_id:T.task_id,format:'recall40-human-truth-v1',exported_at:new Date().toISOString(),parent_count:40,responses};let a=document.createElement('a');a.href=URL.createObjectURL(new Blob([JSON.stringify(out,null,2)],{type:'application/json'}));a.download=T.task_id+'_results.json';a.click()};
document.addEventListener('keydown',e=>{if(e.target===el('src'))return;if(e.key==='ArrowRight')el('next').click();if(e.key==='ArrowLeft')el('prev').click()});draw();
</script>'''

def main()->int:
    ap=argparse.ArgumentParser();ap.add_argument('--output-dir',type=Path,required=True);a=ap.parse_args();t=build_task();a.output_dir.mkdir(parents=True,exist_ok=True);(a.output_dir/'task.json').write_text(json.dumps(t,ensure_ascii=False,sort_keys=True,separators=(',',':'))+'\n');(a.output_dir/'index.html').write_text(render(t));return 0
if __name__=='__main__': raise SystemExit(main())
