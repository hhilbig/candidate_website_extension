#!/usr/bin/env python3
"""Compare corrected URL leads with one completed, frozen Wayback corpus."""
import argparse
import collections
import csv
import datetime as dt
import hashlib
import json
from pathlib import Path
import subprocess
from urllib.parse import urlsplit, urlunsplit


def read_csv(path):
    with Path(path).open(newline="", encoding="utf-8-sig") as handle:
        yield from csv.DictReader(handle)


def sha(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def exact(url):
    """Normalize host case and empty root path; preserve scheme, path and query."""
    p = urlsplit(url)
    return urlunsplit((p.scheme.lower(), p.netloc.lower(), p.path or "/", p.query, ""))


def truth(value):
    return str(value).lower() == "true"


def aggregate(root, frozen, requested):
    root = Path(root)
    targets = {r["target_id"]: r for r in read_csv(frozen)}
    stats = {key: collections.Counter() for key in targets}
    wanted = {(r["candidate_year_id"], exact(r["exact_normalized_url"])) for r in requested}
    exact_stats = {key: collections.Counter() for key in wanted}
    sets = {}
    duplicate = {}
    unexpected_targets = set()
    hashes = {str(frozen): sha(frozen)}
    mismatched_context = 0
    for filename, stage in [("capture_inventory.csv", "inventory"),
                            ("fetch_queue.csv", "queue"), ("fetch_attempts.csv", "fetch")]:
        path = root / filename
        hashes[str(path)] = sha(path)
        seen = set()
        duplicates = 0
        for r in read_csv(path):
            cid, tid = r["capture_id"], r["target_id"]
            duplicates += cid in seen
            seen.add(cid)
            if tid not in targets:
                unexpected_targets.add(tid)
                continue
            target = targets[tid]
            mismatched_context += r["candidate_year_id"] != target["candidate_year_id"]
            count = stats[tid]
            key = (r["candidate_year_id"], exact(r["original_url"]))
            applicable = [count] + ([exact_stats[key]] if key in exact_stats else [])
            for counter in applicable:
                counter[stage] += 1
                if stage == "fetch":
                    status = r["fetch_status"]
                    counter[status] += 1
                    counter["retryable"] += truth(r["retryable"])
                    if status in {"fetched", "fetched_timestamp_mismatch"}:
                        counter["saved_response"] += 1
                        counter["raw_bytes"] += int(r["raw_html_bytes"] or 0)
                        counter["text_characters"] += int(r["text_characters"] or 0)
                        stamp = r.get("effective_timestamp") or r["timestamp"]
                        date = dt.datetime.strptime(stamp[:8], "%Y%m%d").date()
                        election = dt.date.fromisoformat(target["election_reference_date"])
                        counter["saved_within_365_days"] += abs((date-election).days) <= 365
                        counter["missing_raw_file"] += not (root / r["raw_html_path"]).is_file()
                        counter["missing_text_file"] += not (root / r["text_path"]).is_file()
        sets[stage] = seen
        duplicate[stage] = duplicates
    journal = root / "fetch_attempts.journal.csv"
    journal_rows = sum(1 for _ in read_csv(journal)) if journal.exists() else 0
    if journal_rows:
        raise RuntimeError("Active journal requires explicit state reduction before reconciliation")
    status_file = Path(frozen).parent.parent / "runs/production/inventory_target_status.csv"
    target_status = {r["target_id"]: r for r in read_csv(status_file)}
    hashes[str(status_file)] = sha(status_file)
    rows = []
    for tid, target in targets.items():
        rows.append({**target, **dict(stats[tid]), "inventory_target_status":
                     target_status.get(tid, {}).get("target_status", "unknown")})
    prior = []
    prior_exact = {key: collections.Counter() for key in wanted}
    acquisition_root = Path(frozen).parents[2]
    for batch in ["covered_151_20260826", "openai_residual_100_20260826",
                  "openai_residual_expansion_357_20260826", "full_discovery_740_20260826"]:
        batch_root = acquisition_root / batch
        checkpoint = batch_root / "runs/production/fetch_attempts.csv"
        frozen_prior = batch_root / "frozen/targets.csv"
        if not checkpoint.exists() or not frozen_prior.exists():
            prior.append({"batch":batch,"availability":"missing_expected_checkpoint_or_frozen_table"})
            continue
        hashes[str(checkpoint)] = sha(checkpoint)
        hashes[str(frozen_prior)] = sha(frozen_prior)
        prior_targets = {r["target_id"]: r for r in read_csv(frozen_prior)}
        counters = collections.defaultdict(collections.Counter)
        for r in read_csv(checkpoint):
            tid = r["target_id"]
            counter = counters[tid]
            counter["checkpoint_rows"] += 1
            if r["fetch_status"] not in {"fetched", "fetched_timestamp_mismatch"}:
                continue
            counter["saved_response"] += 1
            present = (checkpoint.parent / r["raw_html_path"]).is_file()
            counter["raw_file_present"] += present
            key = r["candidate_year_id"], exact(r["original_url"])
            if key in prior_exact:
                prior_exact[key]["saved_response"] += 1
                prior_exact[key]["raw_file_present"] += present
        for tid, target in prior_targets.items():
            prior.append({"batch":batch,"availability":"checkpoint_present", "target_id":tid,
                          "candidate_year_id":target["candidate_year_id"],
                          "registrable_domain":target["registrable_domain"], **counters[tid]})
    return {"targets": rows, "prior_targets":prior,
            "prior_exact":[{"candidate_year_id":k[0],"exact_url":k[1],**v} for k,v in prior_exact.items()],
            "exact": [{"candidate_year_id": k[0], "exact_url": k[1], **v}
             for k, v in exact_stats.items()], "hashes": hashes,
            "audit": {"unique_capture_keys": {k: len(v) for k,v in sets.items()},
             "duplicate_capture_keys": duplicate,
             "inventory_not_queued": len(sets["inventory"]-sets["queue"]),
             "unexpected_queue_keys": len(sets["queue"]-sets["inventory"]),
             "missing_fetch_keys": len(sets["queue"]-sets["fetch"]),
             "unexpected_fetch_keys": len(sets["fetch"]-sets["queue"]),
             "unexpected_target_ids": sorted(unexpected_targets),
             "candidate_context_mismatches": mismatched_context,
             "active_journal_rows": journal_rows}}


def write_csv(path, rows):
    rows = list(rows)
    keys = list(dict.fromkeys(k for row in rows for k in row))
    with Path(path).open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=keys)
        writer.writeheader()
        writer.writerows(rows)


def reconcile(urls, roster, remote, validation):
    targets = remote["targets"]
    by_pair = collections.defaultdict(list)
    for target in targets:
        by_pair[target["candidate_year_id"], target["registrable_domain"]].append(target)
    ex = {(r["candidate_year_id"], r["exact_url"]): r for r in remote["exact"]}
    validations = {r["candidate_url_id"]: r for r in validation}
    prior_pairs = collections.defaultdict(list)
    for target in remote.get("prior_targets", []):
        if "candidate_year_id" in target:
            prior_pairs[target["candidate_year_id"],target["registrable_domain"]].append(target)
    prior_exact = {(r["candidate_year_id"],r["exact_url"]):r for r in remote.get("prior_exact",[])}
    roster_by_id = {r["candidate_year_id"]: r for r in roster}
    results = []
    for row in urls:
        r = dict(row)
        matching = by_pair.get((r["candidate_year_id"], r["registrable_domain"]), [])
        e = ex.get((r["candidate_year_id"], exact(r["exact_normalized_url"])), {})
        r["active_2010_2021"] = truth(roster_by_id[r["candidate_year_id"]]["active_2010_2021"])
        r["frozen_target_ids_json"] = json.dumps([t["target_id"] for t in matching])
        for field in ["inventory", "queue", "saved_response", "saved_within_365_days", "retryable"]:
            r["same_candidate_domain_"+field] = sum(t.get(field, 0) for t in matching)
            r["exact_url_"+field] = e.get(field, 0)
        if e.get("saved_response", 0):
            state = "exact_url_response_saved"
        elif r["same_candidate_domain_saved_response"]:
            state = "same_candidate_domain_response_saved"
        elif matching and r["same_candidate_domain_inventory"]:
            state = "same_candidate_domain_metadata_only"
        elif matching:
            state = "frozen_target_no_capture" if all(t["inventory_target_status"] == "no_capture" for t in matching) else "frozen_target_inventory_unresolved"
        else:
            state = "not_in_completed_frozen_batch"
        r["completed_batch_status"] = state
        v = validations.get(r["candidate_url_id"], {})
        r["prior_validation_campaign_supported"] = truth(v.get("historical_campaign_content_supported", ""))
        r["prior_validation_status"] = v.get("final_validation_status", "not_in_120_proposal_validation")
        r["prior_validation_saved_capture_metadata"] = bool(v.get("capture_content_sha256")) and int(v.get("capture_content_bytes") or 0) > 0
        old = prior_pairs.get((r["candidate_year_id"],r["registrable_domain"]), [])
        r["earlier_batches_json"] = json.dumps(sorted({t["batch"] for t in old}))
        r["earlier_same_candidate_domain_saved_rows"] = sum(t.get("saved_response",0) for t in old)
        r["earlier_same_candidate_domain_present_raw_files"] = sum(t.get("raw_file_present",0) for t in old)
        r["earlier_exact_url_present_raw_files"] = prior_exact.get((r["candidate_year_id"],exact(r["exact_normalized_url"])),{}).get("raw_file_present",0)
        r["review_action"] = ("review_saved_exact_response" if e.get("saved_response") else
                              "review_earlier_saved_exact_response" if r["earlier_exact_url_present_raw_files"] else
                              "inspect_saved_validation_capture_and_exact_url_equivalence" if r["prior_validation_saved_capture_metadata"] else
                              "inspect_earlier_saved_domain_pages_and_exact_path_gap" if r["earlier_same_candidate_domain_present_raw_files"] else
                              "inspect_saved_domain_pages_and_exact_path_gap" if r["same_candidate_domain_saved_response"] else
                              "check_prior_acquisition_then_inventory_exact_url" if not matching else
                              "assess_exact_path_metadata_or_terminal_fetch_result")
        results.append(r)
    return results


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="root@droplet")
    parser.add_argument("--remote-root", default="/mnt/mayoral_wayback_full_snapshots/runs/production_all_inventory")
    parser.add_argument("--remote-frozen", default="/root/candidate_website_extension/data/municipal_archive_acquisition/full_discovery_740_20260826/frozen/targets.csv")
    parser.add_argument("--input-dir", type=Path, default=Path("data/municipal_discovery/exhaustive_source_expansion_20260908/ownership_repair_20260910/combined"))
    parser.add_argument("--validation", type=Path, default=Path("quality_reports/mayoral_archive_validation_2026-09-10/reuse_resolution/validation.csv"))
    parser.add_argument("--output", type=Path, default=Path("quality_reports/mayoral_review_reconciliation_2026-09-13/reconciliation"))
    parser.add_argument("--reuse-remote", action="store_true")
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    urls = list(read_csv(args.input_dir / "candidate_urls.csv"))
    roster = list(read_csv(args.input_dir / "candidate_roster.csv"))
    validation = list(read_csv(args.validation))
    assert len({r["candidate_year_id"] for r in roster}) == len(roster)
    assert len({r["candidate_url_id"] for r in urls}) == len(urls)
    cache = args.output / "remote_aggregates.json"
    if args.reuse_remote:
        remote = json.loads(cache.read_text())
    else:
        requests = [{k:r[k] for k in ["candidate_year_id", "exact_normalized_url"]} for r in urls]
        source = Path(__file__).read_text().split("\ndef main():")[0]
        source += "\nprint(json.dumps(aggregate(" + repr(args.remote_root) + "," + repr(args.remote_frozen) + "," + repr(requests) + ")))\n"
        result = subprocess.run(["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=15", args.host, "python3", "-"], input=source, text=True, capture_output=True)
        if result.returncode:
            raise RuntimeError(result.stderr)
        remote = json.loads(result.stdout)
        cache.write_text(json.dumps(remote, indent=2)+"\n")
    rows = reconcile(urls, roster, remote, validation)
    active = [r for r in rows if r["active_2010_2021"]]
    write_csv(args.output / "url_reconciliation.csv", rows)
    write_csv(args.output / "frozen_target_reconciliation.csv", remote["targets"])
    write_csv(args.output / "remaining_acquisition_review_queue.csv", [r for r in active if not r["exact_url_saved_response"] and not r["earlier_exact_url_present_raw_files"]])
    write_csv(args.output / "prior_batch_target_check.csv", remote.get("prior_targets",[]))
    current_pairs = {(r["candidate_year_id"], r["registrable_domain"]) for r in rows}
    stale = [r for r in remote["targets"] if (r["candidate_year_id"],r["registrable_domain"]) not in current_pairs]
    write_csv(args.output / "frozen_targets_absent_from_current_list.csv", stale)
    def counts(items):
        return dict(collections.Counter(r["completed_batch_status"] for r in items))
    candidate_rows = []
    for person in roster:
        own = [r for r in rows if r["candidate_year_id"] == person["candidate_year_id"]]
        candidate_rows.append({k:person[k] for k in ["candidate_year_id", "full_name", "geo_name", "state_abb", "year", "active_2010_2021"]} | {
            "current_url_count": len(own), "any_exact_response_saved": any(r["exact_url_saved_response"] for r in own),
            "any_same_candidate_domain_response_saved": any(r["same_candidate_domain_saved_response"] for r in own),
            "any_inspected_batch_exact_response_saved": any(r["exact_url_saved_response"] or r["earlier_exact_url_present_raw_files"] for r in own),
            "any_inspected_batch_same_candidate_domain_response_saved": any(r["same_candidate_domain_saved_response"] or r["earlier_same_candidate_domain_present_raw_files"] for r in own),
            "any_prior_validation_saved_capture_metadata": any(r["prior_validation_saved_capture_metadata"] for r in own),
            "any_prior_validation_campaign_supported": any(r["prior_validation_campaign_supported"] for r in own),
            "any_url_outside_completed_batch": any(r["completed_batch_status"] == "not_in_completed_frozen_batch" for r in own)})
    write_csv(args.output / "candidate_year_reconciliation.csv", candidate_rows)
    summary = {"created_at_utc":dt.datetime.now(dt.timezone.utc).isoformat(), "remote_host":args.host,
       "remote_root": args.remote_root, "script_sha256": sha(__file__),
       "input_sha256": {str(p):sha(p) for p in [args.input_dir/"candidate_urls.csv",args.input_dir/"candidate_roster.csv", args.validation]},
       "audit":remote["audit"], "roster_rows":len(roster), "current_url_rows":len(rows),
       "active_candidate_years":sum(truth(r["active_2010_2021"]) for r in roster),
       "active_candidate_years_with_urls":len({r["candidate_year_id"] for r in active}),
       "active_url_rows":len(active), "all_url_status_counts":counts(rows), "active_url_status_counts":counts(active),
       "frozen_targets":len(remote["targets"]), "frozen_targets_absent_from_current_list":len(stale),
       "frozen_status_totals":{k:sum(r.get(k,0) for r in remote["targets"]) for k in ["inventory","queue","fetch","fetched","fetched_timestamp_mismatch","no_content","not_available","redirect_loop","saved_response","retryable","missing_raw_file","missing_text_file"]},
       "active_earlier_batch_exact_url_saved_rows":sum(bool(r["earlier_exact_url_present_raw_files"]) for r in active),
       "active_any_inspected_batch_exact_url_saved_rows":sum(bool(r["earlier_exact_url_present_raw_files"] or r["exact_url_saved_response"]) for r in active),
       "active_review_action_counts":dict(collections.Counter(r["review_action"] for r in active)),
       "active_candidate_counts":{k:sum(truth(r["active_2010_2021"]) and bool(r[k]) for r in candidate_rows) for k in ["any_exact_response_saved","any_same_candidate_domain_response_saved","any_inspected_batch_exact_response_saved","any_inspected_batch_same_candidate_domain_response_saved","any_prior_validation_saved_capture_metadata","any_prior_validation_campaign_supported","any_url_outside_completed_batch"]},
       "limitations":["Exact matches preserve scheme, host, path case, and query. They do not equate www variants or http with https.",
        "Exact saved-response matches use the requested original URL. Effective redirects and timestamp substitutions still need content review.",
        "Domain associations require the same candidate-year. Shared hosts and other paths do not establish ownership.",
        "Saved-response status does not prove campaign content. This script does not classify page content.",
        "The completed corpus covers one frozen batch. Older acquisition batches and roster download flags remain separate evidence.",
        "Raw and text file existence is checked for each saved row. Full file hashes are not recomputed.",
        "The prior content-validation table contains 120 proposals. Its positive flag is automated evidence, not human approval.",
        "The remaining queue is for review, not immediate download. Inspect equivalent URLs and saved validation captures before network acquisition."]}
    audit = remote["audit"]
    summary["completed_batch_reconciliation_pass"] = not any([
        *audit["duplicate_capture_keys"].values(), audit["inventory_not_queued"],audit["unexpected_queue_keys"],
        audit["missing_fetch_keys"],audit["unexpected_fetch_keys"],audit["unexpected_target_ids"],
        audit["candidate_context_mismatches"],audit["active_journal_rows"],
        summary["frozen_status_totals"]["missing_raw_file"],summary["frozen_status_totals"]["missing_text_file"]])
    (args.output/"summary.json").write_text(json.dumps(summary,indent=2)+"\n")
    print(json.dumps(summary,indent=2))


if __name__ == "__main__":
    main()
