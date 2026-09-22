"""Fail-closed authorization shell for the frozen 35,213-window expansion."""
from __future__ import annotations

import argparse, json, sys
from pathlib import Path
from typing import Any

REPO=Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path: sys.path.insert(0,str(REPO))
from scripts.build_candidate_endorsement_recall40_human_recall_task import sha
from scripts.score_candidate_endorsement_recall40_human_truth import score
from src.candidate_endorsement_line_ledger import (FROZEN_LINE_LEDGER_CONTRACT_SHA256,
 FROZEN_STAGE_A_V2_CONTRACT_SHA256,MODEL_ID,MODEL_REVISION,RUNTIME_LOCK_SHA256)

TASK=REPO/"build/private_candidate_endorsement_recall40_human_truth_20260828/task.json"
HTML=TASK.parent/"index.html"
TASK_SHA="d7b216398e8e72c8de76c526a78112f70a77933aa94d5794e85bf4f4114e90db"
HTML_SHA="cb92e402b355a7b716c44bad543e9d2aec22173fe4948486008ce9649b956982"
SOURCE_MANIFEST=REPO/"build/hive_candidate_extraction/v2_1_full_export_20260824p/manifest.json"
SOURCE_MANIFEST_SHA="f5e6545a2d2ec35a83f4184e0d328635e5fba9d3c80cd4c8a7466178b7d9172c"
WINDOW_COUNT=35213
SCORER_PATH=REPO/"scripts/score_candidate_endorsement_recall40_human_truth.py"
SCORER_SHA="38b28b74aab4f321f2f45678b90ce937ae8bb679baaffebe414e1c1c367e1b3c"
EXPECTED_STAGE_A_CONTRACT="ac8d6ed7f06e58a7961e08f8d50f7a86907f1bada12f1c70f5e9963011a335c2"
EXPECTED_LINE_LEDGER_CONTRACT="37dd3a17f5b70e377df4e2b9f1c8213d7909b5838cf99b6c28676e7ac7c8c4fc"
EXPECTED_MODEL_ID="Qwen/Qwen3.6-27B-FP8"
EXPECTED_MODEL_REVISION="e89b16ebf1988b3d6befa7de50abc2d76f26eb09"
EXPECTED_RUNTIME_LOCK="4f46101fa493e38b312af034b8d740c612bd0001bab4cb0f624f4a9e1c49f5b3"

def strict(path:Path)->Any:
 def hook(pairs):
  d={}
  for k,v in pairs:
   if k in d: raise ValueError(f"duplicate JSON key: {k}")
   d[k]=v
  return d
 return json.loads(path.read_text(),object_pairs_hook=hook,parse_constant=lambda x:(_ for _ in()).throw(ValueError("nonfinite JSON")))

def authorize(results:Path,report:Path)->dict[str,Any]:
 if not results.is_file() or not report.is_file(): raise ValueError("recall40 human results and GO report are required")
 if sha(SCORER_PATH)!=SCORER_SHA: raise ValueError("frozen recall40 scorer bytes differ")
 if (FROZEN_STAGE_A_V2_CONTRACT_SHA256,FROZEN_LINE_LEDGER_CONTRACT_SHA256,MODEL_ID,MODEL_REVISION,RUNTIME_LOCK_SHA256)!=(EXPECTED_STAGE_A_CONTRACT,EXPECTED_LINE_LEDGER_CONTRACT,EXPECTED_MODEL_ID,EXPECTED_MODEL_REVISION,EXPECTED_RUNTIME_LOCK): raise ValueError("frozen contract/model/runtime constants differ")
 if sha(TASK)!=TASK_SHA or sha(HTML)!=HTML_SHA: raise ValueError("frozen recall40 task/app differ")
 if sha(SOURCE_MANIFEST)!=SOURCE_MANIFEST_SHA: raise ValueError("frozen full source manifest differs")
 source=strict(SOURCE_MANIFEST)
 if type(source.get("exported_records")) is not int or source["exported_records"]!=WINDOW_COUNT: raise ValueError("full source window inventory differs")
 observed=strict(report); expected=score(TASK,results)
 canonical=lambda value:json.dumps(value,ensure_ascii=False,sort_keys=True,separators=(',',':'))
 if canonical(observed)!=canonical(expected): raise ValueError("recall40 GO report differs from exact typed recomputation")
 if observed.get("gate_decision")!="GO_CONSIDER_FULL_PRIVATE_ENDORSEMENT_INFERENCE": raise ValueError("recall40 gate is not GO")
 if set(observed)!={"version","task_sha256","html_sha256","results_sha256","matcher_implementation_sha256","gate_decision","checks","totals","page_scores_sha256","full_private_inference_authorized","release_authorized"}: raise ValueError("recall40 GO report field set differs")
 if observed["version"]!="candidate_endorsement_recall40_human_truth_score_v2" or observed["task_sha256"]!=TASK_SHA or observed["html_sha256"]!=HTML_SHA or observed["results_sha256"]!=sha(results) or observed["matcher_implementation_sha256"]!="c5fc6edad2cd32ffec3d1941f877fbf25ef8d1d8487f62b017bd6c597b24f2ca": raise ValueError("recall40 GO report bindings differ")
 if not isinstance(observed["page_scores_sha256"],str) or len(observed["page_scores_sha256"])!=64 or set(observed["totals"])!={"human","matched","support","true_zero","true_zero_pass","model"} or any(type(v) is not int for v in observed["totals"].values()): raise ValueError("recall40 GO report totals/digest differ")
 if observed.get("full_private_inference_authorized") is not False or observed.get("release_authorized") is not False: raise ValueError("recall40 authorization flags differ")
 checks=observed.get("checks")
 if not isinstance(checks,list) or [c.get("id") for c in checks]!=["deterministic_validity_percent","complete_pages_min","human_truth_presence_percent","combined_record_context_support_percent","unresolved_withheld_duplicate_matches"] or any(not isinstance(c,dict) or set(c)!={"id","observed","expected","passed"} for c in checks) or not all(c.get("passed") is True for c in checks): raise ValueError("recall40 checks differ")
 return {"version":"candidate_extraction_full_35213_authorization_shell_v1","recall40_results_sha256":sha(results),"recall40_go_report_sha256":sha(report),"recall40_scorer_sha256":SCORER_SHA,"source_manifest_sha256":SOURCE_MANIFEST_SHA,"source_window_count":WINDOW_COUNT,"stage_a_v2_contract_sha256":EXPECTED_STAGE_A_CONTRACT,"line_ledger_contract_sha256":EXPECTED_LINE_LEDGER_CONTRACT,"model_id":EXPECTED_MODEL_ID,"model_revision":EXPECTED_MODEL_REVISION,"runtime_lock_sha256":EXPECTED_RUNTIME_LOCK,"recall_gate_passed":True,"full_artifact_materialization_authorized":False,"hive_submission_authorized":False,"release_authorized":False}

def main()->int:
 ap=argparse.ArgumentParser();ap.add_argument('--recall40-results',type=Path,required=True);ap.add_argument('--recall40-go-report',type=Path,required=True);ap.add_argument('--output-dir',type=Path,required=True);a=ap.parse_args()
 if a.output_dir.exists(): raise ValueError("output directory must not exist")
 authorization=authorize(a.recall40_results,a.recall40_go_report)
 # Deliberate shell boundary: successful authentication is necessary but this turn
 # does not authorize materializing the full expansion.
 raise RuntimeError("recall40 GO authenticated; full artifact materialization requires separate authorization: "+json.dumps(authorization,sort_keys=True,separators=(',',':')))
if __name__=='__main__':raise SystemExit(main())
