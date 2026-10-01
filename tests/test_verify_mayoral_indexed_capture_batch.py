import csv
import gzip
import hashlib
import json
from pathlib import Path
import sqlite3

import scripts.verify_mayoral_indexed_capture_batch as verifier


def make_batch(tmp_path, status="fetched"):
    root = tmp_path / "batch"
    (root / "inputs").mkdir(parents=True)
    (root / "run" / "raw").mkdir(parents=True)
    (root / "run" / "text").mkdir()
    row = {"candidate_year_id": "1__2020", "timestamp": "20200101000000",
           "original_url": "https://www.example.org/", "provider": "web.archive.org",
           "candidate_url_id": "candidate_url_1", "inventory_requested_url": "https://example.org/",
           "url_relation": "index_returned_url_variant", "reason": "no_verified_exact_response"}
    row["requested_replay_url"] = f"https://web.archive.org/web/{row['timestamp']}id_/{row['original_url']}"
    row["proposed_capture_key"] = verifier.capture_key(row)
    queue = root / "inputs" / "exact_capture_queue.csv"
    with queue.open("w", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(row))
        writer.writeheader()
        writer.writerow(row)
    manifest = {"contract_version": "mayoral_indexed_capture_batch_v1", "selected_capture_keys": 1,
                "approved_queue_sha256": hashlib.sha256(queue.read_bytes()).hexdigest(),
                "approval": {"status": "approved", "approved_batches": {"index_returned_url_variant": 1}},
                "preserve_original_proposal_bytes": True,
                "approved_url_relations": ["index_returned_url_variant"],
                "approved_limits": verifier.EXPECTED_LIMITS}
    manifest_path = root / "inputs" / "manifest.json"
    manifest_path.write_text(json.dumps(manifest))
    contract = {"contract_version": "mayoral_indexed_capture_batch_v1",
                "queue_sha256": manifest["approved_queue_sha256"], "limits": verifier.EXPECTED_LIMITS,
                "max_redirects": verifier.MAX_REDIRECTS,
                "approval_manifest_sha256": hashlib.sha256(manifest_path.read_bytes()).hexdigest(),
                "proposal_manifest_sha256": None}
    db = sqlite3.connect(root / "run" / "ledger.sqlite3")
    db.executescript("""
      CREATE TABLE contract (id INTEGER PRIMARY KEY, value TEXT NOT NULL);
      CREATE TABLE current (key TEXT PRIMARY KEY,status TEXT,attempts INTEGER,next_due REAL,result TEXT);
      CREATE TABLE attempts (id TEXT PRIMARY KEY,key TEXT,started REAL,finished REAL,status TEXT,result TEXT);
      CREATE TABLE hops (attempt_id TEXT,hop INTEGER,url TEXT,charged INTEGER,decoded INTEGER,result TEXT);
    """)
    db.execute("INSERT INTO contract VALUES (1,?)", (json.dumps(contract),))
    if status == "pending":
        db.execute("INSERT INTO current VALUES (?,?,0,0,NULL)", (row["proposed_capture_key"], status))
    elif status == "started":
        db.execute("INSERT INTO current VALUES (?,?,1,0,NULL)", (row["proposed_capture_key"], status))
        db.execute("INSERT INTO attempts VALUES (?,?,1,NULL,'started',NULL)", ("a1", row["proposed_capture_key"]))
        db.execute("INSERT INTO hops VALUES ('a1',0,?,?,NULL,NULL)", (row["requested_replay_url"], verifier.EXPECTED_LIMITS["max_response_decoded_bytes"]))
    else:
        raw = b"<html><p>saved text</p></html>"
        text = b"saved text"
        (root / "run" / "raw" / "a1-0.gz").write_bytes(gzip.compress(raw, mtime=0))
        (root / "run" / "text" / "a1.txt").write_bytes(text)
        evidence = {"url": row["requested_replay_url"], "status_code": 200,
                    "headers": {"Content-Type": "text/html"}, "decoded_bytes": len(raw),
                    "raw_path": "raw/a1-0.gz", "raw_sha256": hashlib.sha256(raw).hexdigest(), "oversized": False}
        result = {"requested_original_url": row["original_url"], "requested_timestamp": row["timestamp"],
                  "requested_replay_url": row["requested_replay_url"], "redirects": [evidence],
                  "effective_replay_url": row["requested_replay_url"], "effective_timestamp": row["timestamp"],
                  "effective_original_url": row["original_url"], "decoded_bytes": len(raw),
                  "text_path": "text/a1.txt", "text_sha256": hashlib.sha256(text).hexdigest(), "text_bytes": len(text)}
        db.execute("INSERT INTO current VALUES (?,?,1,0,?)", (row["proposed_capture_key"], status, json.dumps(result)))
        db.execute("INSERT INTO attempts VALUES (?,?,1,2,?,?)", ("a1", row["proposed_capture_key"], status, json.dumps(result)))
        db.execute("INSERT INTO hops VALUES (?,?,?,?,?,?)", ("a1", 0, row["requested_replay_url"], len(raw), len(raw), json.dumps(evidence)))
    db.commit()
    db.close()
    return root


def test_complete_indexed_batch(tmp_path):
    root = make_batch(tmp_path)
    result = verifier.verify_batch(root, terminal=True)
    assert result["valid"] and result["acquisition_complete"]
    assert result["statuses"] == {"fetched": 1}
    assert result["raw_files"] == result["text_files"] == 1


def test_partial_batch_is_truthful(tmp_path):
    result = verifier.verify_batch(make_batch(tmp_path, "pending"))
    assert result["valid"]
    assert not result["acquisition_complete"]
    assert result["statuses"] == {"pending": 1}


def test_terminal_rejects_started_reservation(tmp_path):
    result = verifier.verify_batch(make_batch(tmp_path, "started"), terminal=True)
    assert not result["valid"]
    assert any("started attempt" in error for error in result["errors"])


def test_mismatch_cannot_be_counted_as_fetched(tmp_path):
    root = make_batch(tmp_path)
    db = sqlite3.connect(root / "run" / "ledger.sqlite3")
    result = json.loads(db.execute("SELECT result FROM attempts").fetchone()[0])
    result["effective_timestamp"] = "20200202000000"
    result["effective_replay_url"] = result["effective_replay_url"].replace("20200101000000", "20200202000000")
    db.execute("UPDATE attempts SET result=?", (json.dumps(result),))
    db.commit()
    db.close()
    checked = verifier.verify_batch(root, terminal=True)
    assert not checked["valid"]
    assert not checked["acquisition_complete"]


def test_raw_hash_checked_for_non_success(tmp_path):
    root = make_batch(tmp_path, "capture_mismatch")
    raw_path = next((root / "run" / "raw").iterdir())
    raw_path.write_bytes(gzip.compress(b"changed", mtime=0))
    result = verifier.verify_batch(root)
    assert not result["valid"]
    assert any("raw byte count" in error or "raw hash" in error for error in result["errors"])


def test_budget_attempt_limit_and_manifest_binding(tmp_path):
    root = make_batch(tmp_path)
    db = sqlite3.connect(root / "run" / "ledger.sqlite3")
    db.execute("UPDATE current SET attempts=4")
    db.execute("UPDATE contract SET value=?", (json.dumps({"queue_sha256": "bad"}),))
    db.commit()
    db.close()
    result = verifier.verify_batch(root)
    assert not result["valid"]
    assert any("attempt limit" in error for error in result["errors"])
    assert any("manifest binding" in error for error in result["errors"])


def test_current_result_must_match_latest_attempt(tmp_path):
    root = make_batch(tmp_path)
    db = sqlite3.connect(root / "run" / "ledger.sqlite3")
    db.execute("UPDATE current SET result='{}'")
    db.commit()
    db.close()
    assert not verifier.verify_batch(root)["valid"]


def test_fetched_cannot_be_oversized(tmp_path):
    root = make_batch(tmp_path)
    db = sqlite3.connect(root / "run" / "ledger.sqlite3")
    evidence = json.loads(db.execute("SELECT result FROM hops").fetchone()[0])
    evidence["oversized"] = True
    db.execute("UPDATE hops SET charged=?,result=?", (verifier.EXPECTED_LIMITS["max_response_decoded_bytes"], json.dumps(evidence)))
    result = json.loads(db.execute("SELECT result FROM attempts").fetchone()[0])
    result["redirects"] = [evidence]
    db.execute("UPDATE attempts SET result=?", (json.dumps(result),))
    db.execute("UPDATE current SET result=?", (json.dumps(result),))
    db.commit()
    db.close()
    checked = verifier.verify_batch(root)
    assert not checked["valid"]
    assert any("oversized" in error for error in checked["errors"])


def test_replay_identity_preserves_empty_query():
    url = "https://web.archive.org/web/20200101000000id_/https://example.org/?"
    assert verifier.replay_identity(url) == ("20200101000000", "https://example.org/?")


def test_retryable_timeout_reservation_is_valid_partial(tmp_path):
    root = make_batch(tmp_path, "started")
    db = sqlite3.connect(root / "run" / "ledger.sqlite3")
    result = json.dumps({"error": "ReadTimeout: timed out"})
    db.execute("UPDATE attempts SET status='retryable',finished=2,result=?", (result,))
    db.execute("UPDATE current SET status='retryable',result=?", (result,))
    db.commit()
    db.close()
    checked = verifier.verify_batch(root, terminal=True)
    assert checked["valid"], checked["errors"]
    assert not checked["acquisition_complete"]


def test_interrupted_attempt_is_valid_crash_recovery_partial(tmp_path):
    root = make_batch(tmp_path, "started")
    db = sqlite3.connect(root / "run" / "ledger.sqlite3")
    db.execute("UPDATE attempts SET status='interrupted',finished=2")
    db.execute("UPDATE current SET status='retryable',next_due=30")
    db.commit(); db.close()
    checked = verifier.verify_batch(root, terminal=True)
    assert checked["valid"], checked["errors"]


def test_retryable_timeout_after_completed_redirect_hop_is_valid(tmp_path):
    root = make_batch(tmp_path)
    db = sqlite3.connect(root / "run" / "ledger.sqlite3")
    error = json.dumps({"error": "ReadTimeout: timed out"})
    db.execute("UPDATE attempts SET status='retryable',result=?", (error,))
    db.execute("UPDATE current SET status='retryable',result=?", (error,))
    db.commit(); db.close()
    checked = verifier.verify_batch(root, terminal=True)
    assert checked["valid"], checked["errors"]


def test_started_current_may_retain_previous_result(tmp_path):
    root = make_batch(tmp_path, "started")
    db = sqlite3.connect(root / "run" / "ledger.sqlite3")
    db.execute("UPDATE current SET result=?", (json.dumps({"error": "previous"}),))
    db.commit(); db.close()
    checked = verifier.verify_batch(root)
    assert checked["valid"], checked["errors"]


def test_fetched_effective_url_must_equal_final_hop(tmp_path):
    root = make_batch(tmp_path)
    db = sqlite3.connect(root / "run" / "ledger.sqlite3")
    result = json.loads(db.execute("SELECT result FROM attempts").fetchone()[0])
    result["effective_replay_url"] = result["effective_replay_url"].replace("id_", "")
    db.execute("UPDATE attempts SET result=?", (json.dumps(result),))
    db.execute("UPDATE current SET result=?", (json.dumps(result),))
    db.commit(); db.close()
    checked = verifier.verify_batch(root)
    assert not checked["valid"]
    assert any("final hop" in error for error in checked["errors"])


def test_evidence_paths_must_follow_runner_contract(tmp_path):
    root = make_batch(tmp_path)
    db = sqlite3.connect(root / "run" / "ledger.sqlite3")
    evidence = json.loads(db.execute("SELECT result FROM hops").fetchone()[0])
    evidence["raw_path"] = "../raw/a1-0.gz"
    db.execute("UPDATE hops SET result=?", (json.dumps(evidence),))
    result = json.loads(db.execute("SELECT result FROM attempts").fetchone()[0])
    result["redirects"] = [evidence]
    result["text_path"] = "../text/a1.txt"
    db.execute("UPDATE attempts SET result=?", (json.dumps(result),))
    db.execute("UPDATE current SET result=?", (json.dumps(result),))
    db.commit(); db.close()
    checked = verifier.verify_batch(root)
    assert not checked["valid"]
    assert any("runner contract" in error for error in checked["errors"])
