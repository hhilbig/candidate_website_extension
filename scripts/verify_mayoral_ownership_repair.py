#!/usr/bin/env python3
"""Rebuild fixed mayoral claims and audit their changes using saved evidence."""
import argparse
import csv
import hashlib
import json
import sys
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts import expand_mayoral_saved_sources as runner
from scripts.audit_mayoral_source_leads import build_audit, CHANGE_TYPES


def table_hash(db, table):
    digest = hashlib.sha256()
    count = 0
    for row in db.execute(f'SELECT * FROM {table} ORDER BY 1'):
        digest.update((json.dumps(tuple(row), ensure_ascii=False) + '\n').encode())
        count += 1
    return {"rows": count, "sha256": digest.hexdigest()}


def verify(run, prior, output, rebuild=True):
    output.mkdir(parents=True, exist_ok=True)
    db = runner.connect(run)
    assert db.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
    tables = ("sources", "contexts", "attempts", "current", "candidates", "deferred_links")
    before = {table: table_hash(db, table) for table in tables}
    keys_before = {row[0] for row in db.execute("SELECT claim_id FROM claims")}
    if rebuild:
        repair = runner.repair_saved_ownership(db, run)
        runner.atomic_json(run / "ownership_repair.json", repair)
        runner.export(run, db)
    after = {table: table_hash(db, table) for table in tables}
    assert before == after, "Acquisition state changed"
    assert keys_before == {row[0] for row in db.execute("SELECT claim_id FROM claims")}
    assert db.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
    db.close()
    build_audit(run / "combined/new_candidate_urls.csv", str(run / "expanded_url_claims.csv"), output, sample_per_type=0)
    evidence = runner.read_csv(output / "lead_evidence.csv")
    new_ids = {row["candidate_url_id"] for row in evidence}
    old = runner.read_csv(prior / "lead_evidence.csv")
    decisions = runner.read_csv(prior / "audit_decisions.csv")
    comparison = []
    for row in decisions:
        retained = row["candidate_url_id"] in new_ids
        comparison.append({**row, "retained": retained, "expected_retained": row["audit_decision"] == "accept", "matches_audit": retained == (row["audit_decision"] == "accept")})
    runner.atomic_csv(output / "original_sample_comparison.csv", comparison)
    excluded_ids = {row["candidate_url_id"] for row in decisions}
    sample = []
    for kind in CHANGE_TYPES:
        eligible = [row for row in evidence if row["candidate_url_id"] not in excluded_ids and row["summary_change_type"] == kind]
        sample.extend(sorted(eligible, key=lambda row: hashlib.sha256(row["candidate_url_id"].encode()).hexdigest())[:10])
    runner.atomic_csv(output / "fresh_sample.csv", sample)
    report = {"acquisition_tables_before": before, "acquisition_tables_after": after,
              "exact_claim_key_agreement": True, "claim_rows": len(keys_before),
              "prior_proposals": len(old), "repaired_proposals": len(evidence),
              "retained_prior_proposals": len(new_ids & {row["candidate_url_id"] for row in old}),
              "original_sample_outcomes": dict(Counter((row["audit_decision"] + (":retained" if row["retained"] else ":excluded")) for row in comparison)),
              "original_sample_mismatches": [row["candidate_url_id"] for row in comparison if not row["matches_audit"]],
              "fresh_sample_rows": len(sample), "fresh_sample_sha256": runner.sha(output / "fresh_sample.csv"),
              "prior_evidence_sha256": runner.sha(prior / "lead_evidence.csv")}
    runner.atomic_json(output / "repair_verification.json", report)
    return report


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-dir", type=Path, required=True)
    parser.add_argument("--prior-audit-dir", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--verify-only", action="store_true")
    args = parser.parse_args()
    import logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    print(json.dumps(verify(args.run_dir, args.prior_audit_dir, args.output_dir, not args.verify_only), indent=2))
