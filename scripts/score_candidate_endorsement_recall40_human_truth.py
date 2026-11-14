"""Authenticate and score the untouched recall40 complete-parent human truth."""
from __future__ import annotations

import argparse, datetime as dt, json, re, sys
from pathlib import Path
from typing import Any

REPO=Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path: sys.path.insert(0,str(REPO))
from scripts.audit_candidate_endorsement_40_parent_feasibility import score_recall_matching
from scripts.build_candidate_endorsement_recall40_human_recall_task import build_task, sha
from scripts.build_candidate_endorsement_recall40_stage_b_residual_handcode import FAILED_BATCH_IDS, authenticated_inputs, proposed_output
from src.candidate_endorsement_line_ledger import RECALL40_STAGE_B_RESIDUAL_PROJECTION_SPEC_SHA256, align_record_extraction_output, computed_recall40_stage_b_residual_contract, sha256_json, validate_record_extraction_output

STAMP=re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$")
MATCHER_SHA256="c5fc6edad2cd32ffec3d1941f877fbf25ef8d1d8487f62b017bd6c597b24f2ca"
TASK_SHA256="d7b216398e8e72c8de76c526a78112f70a77933aa94d5794e85bf4f4114e90db"
HTML_SHA256="cb92e402b355a7b716c44bad543e9d2aec22173fe4948486008ce9649b956982"
def strict(path:Path)->Any:
 def hook(pairs):
  d={}
  for k,v in pairs:
   if k in d: raise ValueError(f"duplicate JSON key: {k}")
   d[k]=v
  return d
 return json.loads(path.read_text(),object_pairs_hook=hook,parse_constant=lambda x:(_ for _ in()).throw(ValueError("nonfinite JSON")))

def merged_alignments()->tuple[dict[str,list[dict[str,Any]]],dict[str,int]]:
 batches,rows=authenticated_inputs(); contract=computed_recall40_stage_b_residual_contract(); out={}
 census={"withheld_record_count":0,"duplicate_model_identity_count":0}; identities=set()
 for bid,row in rows.items():
  if bid in FAILED_BATCH_IDS:
   model=proposed_output(bid,row["model_output"])
   if not validate_record_extraction_output(batches[bid],model,residual_projection_spec_sha256=RECALL40_STAGE_B_RESIDUAL_PROJECTION_SPEC_SHA256)["schema_valid"]: raise ValueError("repair validation differs")
   alignment=align_record_extraction_output(batches[bid],model,line_ledger_contract_sha256=contract["line_ledger_contract_sha256"],residual_projection_spec_sha256=RECALL40_STAGE_B_RESIDUAL_PROJECTION_SPEC_SHA256)
  else:
   if row["technical_pass"] is not True: raise ValueError("unexpected technical failure")
   alignment=row["stage_alignment"]
  census["withheld_record_count"]+=alignment["withheld_record_count"]
  if not alignment["public_coverage_complete"] or alignment["withheld_record_count"]!=0: raise ValueError("alignment closure differs")
  for record in alignment["aligned_records"]:
   identity=(record["stage_b_batch_id"],record["stage_b_slot"],record["model_record_index"])
   if identity in identities: census["duplicate_model_identity_count"]+=1
   identities.add(identity)
  out.setdefault(batches[bid]["parent_document_id"],[]).extend(alignment["aligned_records"])
 return out,census

def score(task_path:Path,results_path:Path)->dict[str,Any]:
 html_path=task_path.parent/"index.html"
 if sha(task_path)!=TASK_SHA256 or sha(html_path)!=HTML_SHA256: raise ValueError("frozen task/app bytes differ")
 if sha(REPO/"scripts/audit_candidate_endorsement_40_parent_feasibility.py")!=MATCHER_SHA256: raise ValueError("matcher implementation differs")
 task=strict(task_path); expected=build_task()
 if task!=expected: raise ValueError("task differs from authenticated source")
 results=strict(results_path)
 if not isinstance(results,dict) or set(results)!={"task_id","format","exported_at","parent_count","responses"}: raise ValueError("result fields differ")
 if results["task_id"]!=task["task_id"] or results["format"]!="recall40-human-truth-v1" or type(results["parent_count"]) is not int or results["parent_count"]!=40: raise ValueError("result identity/count differs")
 stamp=results["exported_at"]
 if not isinstance(stamp,str) or not STAMP.fullmatch(stamp): raise ValueError("timestamp differs")
 try: parsed=dt.datetime.strptime(stamp,"%Y-%m-%dT%H:%M:%S.%fZ")
 except ValueError as e: raise ValueError("timestamp differs") from e
 if parsed.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]+"Z"!=stamp: raise ValueError("timestamp differs")
 parents={p["parent_document_id"]:p for p in task["parents"]}; expected_ids=list(parents)
 responses=results["responses"]
 if not isinstance(responses,list) or [r.get("parent_document_id") for r in responses]!=expected_ids: raise ValueError("response order differs")
 models_by_parent,alignment_census=merged_alignments(); totals={"human":0,"matched":0,"support":0,"true_zero":0,"true_zero_pass":0,"model":0}; pages=[]
 for response in responses:
  if not isinstance(response,dict) or set(response)!={"parent_document_id","true_zero","occurrences"}: raise ValueError("response schema differs")
  pid=response["parent_document_id"]; text=parents[pid]["parent_source_text"]; zero=response["true_zero"]; occ=response["occurrences"]
  if type(zero) is not bool or not isinstance(occ,list) or zero==(len(occ)>0): raise ValueError("true-zero/occurrence closure differs")
  humans=[]; seen=set()
  for o in occ:
   if not isinstance(o,dict) or set(o)!={"name_start","name_end","support_start","support_end"} or any(type(o[k]) is not int for k in o): raise ValueError("occurrence schema differs")
   ns,ne,ss,se=(o[k] for k in ("name_start","name_end","support_start","support_end"))
   if not (0<=ns<ne<=len(text) and 0<=ss<se<=len(text)): raise ValueError("occurrence offsets differ")
   key=(ns,ne)
   if key in seen: raise ValueError("duplicate human occurrence")
   seen.add(key); humans.append({"endorser_name":text[ns:ne],"name_start":ns,"name_end":ne,"support_start":ss,"support_end":se})
  models=[]
  for r in models_by_parent.get(pid,[]):
   cs=r.get("endorsement_context_evidence_parent_start_char");ce=r.get("endorsement_context_evidence_parent_end_char")
   models.append({"endorser_name":r["endorser_name"],"record_evidence_parent_span":[r["record_evidence_parent_start_char"],r["record_evidence_parent_end_char"]],"endorsement_context_parent_span":[cs,ce] if type(cs) is int and type(ce) is int else [-1,-1],"batch_id":r["stage_b_batch_id"],"slot":r["stage_b_slot"],"record_index":r["model_record_index"]})
  matched=score_recall_matching(humans,models) if humans else {"matched_count":0,"combined_support_covered_count":0}
  zero_pass=zero and len(models)==0
  totals["human"]+=len(humans);totals["matched"]+=matched["matched_count"];totals["support"]+=matched["combined_support_covered_count"];totals["true_zero"]+=zero;totals["true_zero_pass"]+=zero_pass;totals["model"]+=len(models)
  unmatched_models=len(models)-matched["matched_count"]
  pages.append({"parent_document_id":pid,"human_occurrences":len(humans),"model_records":len(models),"matched":matched["matched_count"],"unmatched_model_records":unmatched_models,"support_covered":matched["combined_support_covered_count"],"true_zero":zero,"page_pass":zero_pass if zero else matched["matched_count"]==len(humans) and unmatched_models==0})
 complete=sum(p["page_pass"] for p in pages); presence=100*totals["matched"]/totals["human"] if totals["human"] else 100.0; support=100*totals["support"]/totals["matched"] if totals["matched"] else 100.0; unresolved=(totals["human"]-totals["matched"]+alignment_census["withheld_record_count"]+alignment_census["duplicate_model_identity_count"]); deterministic=100*len(pages)/40
 checks=[{"id":"deterministic_validity_percent","observed":deterministic,"expected":100,"passed":deterministic==100},{"id":"complete_pages_min","observed":complete,"expected":36,"passed":complete>=36},{"id":"human_truth_presence_percent","observed":presence,"expected":100,"passed":presence==100},{"id":"combined_record_context_support_percent","observed":support,"expected":100,"passed":support==100},{"id":"unresolved_withheld_duplicate_matches","observed":unresolved,"expected":0,"passed":unresolved==0}]
 go=all(c["passed"] for c in checks)
 return {"version":"candidate_endorsement_recall40_human_truth_score_v2","task_sha256":sha(task_path),"html_sha256":sha(html_path),"results_sha256":sha(results_path),"matcher_implementation_sha256":MATCHER_SHA256,"gate_decision":"GO_CONSIDER_FULL_PRIVATE_ENDORSEMENT_INFERENCE" if go else "NO_GO","checks":checks,"totals":totals,"page_scores_sha256":sha256_json(pages),"full_private_inference_authorized":False,"release_authorized":False}

def main()->int:
 ap=argparse.ArgumentParser();ap.add_argument('--task',type=Path,required=True);ap.add_argument('--results',type=Path,required=True);ap.add_argument('--output',type=Path,required=True);a=ap.parse_args();r=score(a.task,a.results);a.output.write_text(json.dumps(r,sort_keys=True,separators=(',',':'))+'\n');return 0 if r['gate_decision'].startswith('GO_') else 2
if __name__=='__main__':raise SystemExit(main())
