from __future__ import annotations
import json, subprocess, sys
from pathlib import Path
import pytest
import scripts.build_candidate_endorsement_recall40_human_recall_task as builder
import scripts.score_candidate_endorsement_recall40_human_truth as scorer
from collections import defaultdict
from scripts.audit_candidate_endorsement_40_parent_feasibility import authenticated_full_rows
from src.candidate_endorsement_line_ledger import (
    _projected_record_evidence,
    reconstruct_parent_inventory,
)

REPO=Path(__file__).resolve().parents[1]
DIR=REPO/"build/private_candidate_endorsement_recall40_human_truth_20260828"
TASK=DIR/"task.json"; HTML=DIR/"index.html"

def zero_results():
 t=json.loads(TASK.read_text())
 return {"task_id":t["task_id"],"format":"recall40-human-truth-v1","exported_at":"2026-08-28T12:00:00.000Z","parent_count":40,"responses":[{"parent_document_id":p["parent_document_id"],"true_zero":True,"occurrences":[]} for p in t["parents"]]}

def test_private_task_reproduces_complete_frozen_parent_scope():
 t=json.loads(TASK.read_text()); assert t==builder.build_task(); assert builder.render(t)==HTML.read_text()
 assert len(t["parents"])==40==len({p["parent_document_id"] for p in t["parents"]})
 assert all(len(p["parent_source_text"])==p["source_n_chars"] for p in t["parents"])
 assert "model_output" not in TASK.read_text() and "aligned_records" not in TASK.read_text()
 assert builder.sha(DIR/"results.json")=="0192339258ad40cd8b5b341c16c236e2222c5d014def2a7e4744e09041fb76db"

def test_html_script_parses_and_is_complete_only():
 code="const fs=require('fs'),h=fs.readFileSync(process.argv[1],'utf8'),s=h.match(/<script>([\\s\\S]*?)<\\/script>/g).pop().replace(/^<script>|<\\/script>$/g,'');new Function(s)"
 r=subprocess.run(["node","-e",code,str(HTML)],capture_output=True,text=True)
 assert r.returncode==0,r.stderr
 assert "Complete all 40 parents first" in HTML.read_text()
 html=HTML.read_text()
 assert '<pre id="src" class="src" tabindex="0"></pre>' in html
 assert "function selectedSpan()" in html and "function rememberSpan()" in html
 assert "e=>e.preventDefault()" in html
 assert "selectionStart" not in html and "<textarea" not in html
 r=subprocess.run(["node","-e","const cp=(s,n)=>Array.from(s.slice(0,n)).length;if(cp('A😀B',3)!==2)process.exit(1)"])
 assert r.returncode==0

def test_scorer_runs_fail_closed_without_human_occurrences(tmp_path:Path):
 p=tmp_path/"results.json";p.write_text(json.dumps(zero_results()))
 report=scorer.score(TASK,p)
 assert report["gate_decision"]=="NO_GO" and report["totals"]["model"]==362
 assert report["checks"][-1]["observed"]==0
 assert report["full_private_inference_authorized"] is False and report["release_authorized"] is False

def test_result_order_float_offsets_and_duplicate_keys_reject(tmp_path:Path):
 x=zero_results();x["responses"].reverse();p=tmp_path/"r.json";p.write_text(json.dumps(x))
 with pytest.raises(ValueError,match="order"):scorer.score(TASK,p)
 x=zero_results();x["responses"][0]={"parent_document_id":x["responses"][0]["parent_document_id"],"true_zero":False,"occurrences":[{"name_start":0.0,"name_end":1,"support_start":0,"support_end":1}]};p.write_text(json.dumps(x))
 with pytest.raises(ValueError,match="schema"):scorer.score(TASK,p)
 raw=json.dumps(zero_results());p.write_text(raw[:-1]+',"parent_count":40}')
 with pytest.raises(ValueError,match="duplicate"):scorer.score(TASK,p)

def test_unmatched_model_record_blocks_page_and_gate(tmp_path:Path,monkeypatch):
 x=zero_results();t=json.loads(TASK.read_text());first=t["parents"][0];text=first["parent_source_text"]
 x["responses"][0]={"parent_document_id":first["parent_document_id"],"true_zero":False,"occurrences":[{"name_start":0,"name_end":1,"support_start":0,"support_end":1}]}
 base={"endorser_name":text[:1],"record_evidence_parent_start_char":0,"record_evidence_parent_end_char":1,"endorsement_context_evidence_parent_start_char":0,"endorsement_context_evidence_parent_end_char":1,"stage_b_slot":"slot_00","model_record_index":0}
 models=[{**base,"stage_b_batch_id":"a"},{**base,"stage_b_batch_id":"b","model_record_index":1}]
 monkeypatch.setattr(scorer,"merged_alignments",lambda:({first["parent_document_id"]:models},{"withheld_record_count":0,"duplicate_model_identity_count":0}))
 p=tmp_path/"r.json";p.write_text(json.dumps(x));report=scorer.score(TASK,p)
 assert report["checks"][1]["observed"]==39 and report["checks"][1]["passed"] is True
 assert report["checks"][-1]["observed"]==0 and report["checks"][-1]["passed"] is True

def test_frozen_matcher_collapses_ascii_spaces():
 h={"endorser_name":"Alice  Smith","name_start":0,"name_end":12,"support_start":0,"support_end":12}
 m={"endorser_name":"Alice Smith","record_evidence_parent_span":[0,12],"endorsement_context_parent_span":[0,12],"batch_id":"b","slot":"slot_00","record_index":0}
 assert scorer.score_recall_matching([h],[m])["matched_count"]==1

def test_duplicate_model_identity_populates_unresolved_gate(tmp_path:Path,monkeypatch):
 x=zero_results();p=tmp_path/"r.json";p.write_text(json.dumps(x))
 monkeypatch.setattr(scorer,"merged_alignments",lambda:({},{"withheld_record_count":0,"duplicate_model_identity_count":1}))
 report=scorer.score(TASK,p)
 assert report["checks"][-1]=={"id":"unresolved_withheld_duplicate_matches","observed":1,"expected":0,"passed":False}


def test_round2_scope_exposes_explicit_endorsement_blocks_without_changing_frozen_scope():
 rows,_=authenticated_full_rows(); by=defaultdict(list)
 targets={
  "e8d524546ed19debce50f791a7d7d11a751b63bdae2be3d8bc032e6793ca35f7":
      {"Texas AFL CIO","Princeton Democrats","Collin County Young Democrats"},
  "bce9e4b9897bed665772197d7bf84219b189ce5c628b4cabe80eb70dcbc6af17":
      {"President Donald Trump"},
 }
 for row in rows:
  if row.get("parent_document_id") in targets: by[row["parent_document_id"]].append(row)
 for pid,names in targets.items():
  frozen=reconstruct_parent_inventory(by[pid])
  revised=reconstruct_parent_inventory(by[pid],round2_route_revision=True)
  frozen_visible={l["line_text"] for l in frozen["lines"] if l["model_decision_required"]}
  revised_visible={l["line_text"] for l in revised["lines"] if l["model_decision_required"]}
  assert not names <= frozen_visible
  assert names <= revised_visible


def test_round2_record_projection_keeps_owned_testimonial_support():
 text='Mayor Jane Doe\n“Jane endorses the candidate because she delivers.”\n'
 target={"source_line_id":"a","record_block_lines":[
  {"source_line_id":"a","parent_start_char":0,"parent_end_char":14,"parent_full_end_char":15,"line_text":"Mayor Jane Doe"},
  {"source_line_id":"b","parent_start_char":15,"parent_end_char":66,"parent_full_end_char":67,"line_text":"“Jane endorses the candidate because she delivers.”"},
 ]}
 minimal,_,minimal_end=_projected_record_evidence(text,target,{"endorser_name":"Jane Doe","title":"Mayor","affiliation":None,"jurisdiction":None})
 complete,start,end=_projected_record_evidence(text,target,{"endorser_name":"Jane Doe","title":"Mayor","affiliation":None,"jurisdiction":None},complete_authenticated_block=True)
 assert "endorses" not in minimal and minimal_end==14
 assert start==0 and end==67 and "endorses" in complete
