from __future__ import annotations
import json, subprocess, sys
from pathlib import Path
import pytest
import scripts.build_candidate_extraction_full_expansion_after_recall40 as shell

def go_report(results):
 ids=["deterministic_validity_percent","complete_pages_min","human_truth_presence_percent","combined_record_context_support_percent","unresolved_withheld_duplicate_matches"]
 return {"version":"candidate_endorsement_recall40_human_truth_score_v2","task_sha256":shell.TASK_SHA,"html_sha256":shell.HTML_SHA,"results_sha256":shell.sha(results),"matcher_implementation_sha256":"c5fc6edad2cd32ffec3d1941f877fbf25ef8d1d8487f62b017bd6c597b24f2ca","gate_decision":"GO_CONSIDER_FULL_PRIVATE_ENDORSEMENT_INFERENCE","checks":[{"id":x,"observed":100 if x!="complete_pages_min" and x!="unresolved_withheld_duplicate_matches" else (40 if x=="complete_pages_min" else 0),"expected":100 if x!="complete_pages_min" and x!="unresolved_withheld_duplicate_matches" else (36 if x=="complete_pages_min" else 0),"passed":True} for x in ids],"totals":{"human":360,"matched":360,"support":360,"true_zero":10,"true_zero_pass":10,"model":362},"page_scores_sha256":"0"*64,"full_private_inference_authorized":False,"release_authorized":False}

def test_absent_results_fail_before_output_directory(tmp_path:Path):
 out=tmp_path/"out"
 with pytest.raises(ValueError,match="required"):shell.authorize(tmp_path/"missing-results",tmp_path/"missing-report")
 assert not out.exists()

def test_no_go_and_report_tamper_reject(tmp_path:Path,monkeypatch):
 results=tmp_path/"results.json";report=tmp_path/"report.json";results.write_text("{}")
 expected=go_report(results);expected["gate_decision"]="NO_GO";monkeypatch.setattr(shell,"score",lambda *_:expected)
 report.write_text(json.dumps(expected))
 with pytest.raises(ValueError,match="not GO"):shell.authorize(results,report)
 expected=go_report(results);monkeypatch.setattr(shell,"score",lambda *_:expected)
 attacked={**expected,"checks":[*expected["checks"],{"id":"forged","passed":True}]};report.write_text(json.dumps(attacked))
 with pytest.raises(ValueError,match="exact typed recomputation"):shell.authorize(results,report)
 attacked={**expected,"checks":[dict(c) for c in expected["checks"]],"totals":{"human":40.0}}
 expected={**expected,"totals":{"human":40}}
 monkeypatch.setattr(shell,"score",lambda *_:expected);report.write_text(json.dumps(attacked))
 with pytest.raises(ValueError,match="exact typed recomputation"):shell.authorize(results,report)

def test_exact_go_binds_full_source_runtime_and_never_authorizes_hive_or_release(tmp_path:Path,monkeypatch):
 results=tmp_path/"results.json";report=tmp_path/"report.json";results.write_text("{}")
 expected=go_report(results);report.write_text(json.dumps(expected));monkeypatch.setattr(shell,"score",lambda *_:expected)
 auth=shell.authorize(results,report)
 assert auth["source_window_count"]==35213
 assert auth["source_manifest_sha256"]==shell.SOURCE_MANIFEST_SHA
 assert auth["stage_a_v2_contract_sha256"] and auth["line_ledger_contract_sha256"]
 assert auth["model_id"] and auth["model_revision"] and auth["runtime_lock_sha256"]
 assert auth["recall_gate_passed"] is True
 assert auth["full_artifact_materialization_authorized"] is False
 assert auth["hive_submission_authorized"] is False and auth["release_authorized"] is False

def test_cli_shell_does_not_create_output_on_absent_inputs(tmp_path:Path):
 out=tmp_path/"never-created"
 result=subprocess.run([sys.executable,"scripts/build_candidate_extraction_full_expansion_after_recall40.py","--recall40-results",str(tmp_path/"x"),"--recall40-go-report",str(tmp_path/"y"),"--output-dir",str(out)],cwd=shell.REPO,capture_output=True,text=True)
 assert result.returncode!=0 and "required" in result.stderr
 assert not out.exists()

def test_scorer_and_runtime_constant_drift_reject(tmp_path:Path,monkeypatch):
 results=tmp_path/"results.json";report=tmp_path/"report.json";results.write_text("{}")
 expected=go_report(results);report.write_text(json.dumps(expected));monkeypatch.setattr(shell,"score",lambda *_:expected)
 monkeypatch.setattr(shell,"SCORER_SHA","0"*64)
 with pytest.raises(ValueError,match="scorer bytes"):shell.authorize(results,report)
 monkeypatch.setattr(shell,"SCORER_SHA",shell.sha(shell.SCORER_PATH));monkeypatch.setattr(shell,"MODEL_ID","drift")
 with pytest.raises(ValueError,match="constants differ"):shell.authorize(results,report)

def test_minimal_self_consistent_go_rejects(tmp_path:Path,monkeypatch):
 results=tmp_path/"results.json";report=tmp_path/"report.json";results.write_text("{}")
 minimal={"gate_decision":"GO_CONSIDER_FULL_PRIVATE_ENDORSEMENT_INFERENCE","checks":[{"id":x,"passed":True} for x in ["deterministic_validity_percent","complete_pages_min","human_truth_presence_percent","combined_record_context_support_percent","unresolved_withheld_duplicate_matches"]],"full_private_inference_authorized":False,"release_authorized":False}
 monkeypatch.setattr(shell,"score",lambda *_:minimal);report.write_text(json.dumps(minimal))
 with pytest.raises(ValueError,match="field set"):shell.authorize(results,report)
