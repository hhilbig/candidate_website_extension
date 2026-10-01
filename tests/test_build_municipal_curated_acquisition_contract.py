from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pandas as pd
import pytest

from scripts.build_municipal_curated_acquisition_contract import (
    ATTEMPT_COLUMNS,
    CANDIDATE_TARGET_COLUMNS,
    CONTRACT_VERSION,
    DEFAULT_CANDIDATES,
    DEFAULT_PRIORITY_MANIFEST,
    DEFAULT_SPECS,
    DEFAULT_TARGETS,
    EVIDENCE_COLUMNS,
    FILE_COLUMNS,
    TARGET_COLUMNS,
    AcquisitionContractError,
    audit_post_shard_directories,
    audit_post_shard_frames,
    build_contract,
)


def _real_contract() -> dict[str, object]:
    paths = {
        "targets": DEFAULT_TARGETS,
        "candidate_targets": DEFAULT_CANDIDATES,
        "source_specs": DEFAULT_SPECS,
        "priority_manifest": DEFAULT_PRIORITY_MANIFEST,
    }
    return build_contract(
        pd.read_csv(DEFAULT_TARGETS),
        pd.read_csv(DEFAULT_CANDIDATES),
        json.loads(DEFAULT_SPECS.read_text(encoding="utf-8")),
        json.loads(DEFAULT_PRIORITY_MANIFEST.read_text(encoding="utf-8")),
        paths,
    )


def _valid_outputs(
    results: dict[str, object], tmp_path: Path
) -> tuple[dict[int, Path], pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    contract = results["targets"]
    candidates = results["candidate_targets"]
    found = contract.iloc[0]
    found_candidate = candidates.loc[candidates["target_id"].eq(found["target_id"])].iloc[0]
    content = b"<html><a href='https://Campaign.Example/Mayor'>Campaign site</a></html>"
    digest = hashlib.sha256(content).hexdigest()
    file_id = f"sha256:{digest}"

    roots = {shard: tmp_path / f"shard_{shard}" for shard in (0, 1)}
    for root in roots.values():
        (root / "files").mkdir(parents=True)
    local_path = "files/source-page.html"
    (roots[int(found["shard_id"])] / local_path).write_bytes(content)

    attempts = []
    for row in contract.itertuples(index=False):
        is_found = row.target_id == found["target_id"]
        attempts.append(
            {
                "contract_version": CONTRACT_VERSION,
                "attempt_key": row.attempt_key,
                "target_id": row.target_id,
                "source_id": row.source_id,
                "parse_group_id": row.parse_group_id,
                "shard_id": row.shard_id,
                "partition_hostname": row.partition_hostname,
                "terminal_status": "found" if is_found else "no_result",
                "attempted_at_utc": "2026-08-25T12:00:00Z",
                "retry_count": 0,
                "http_status": 200 if is_found else "",
                "request_url": row.exact_source_url if is_found else "",
                "final_url": row.exact_source_url if is_found else "",
                "downloaded_file_n": 1 if is_found else 0,
                "downloaded_file_ids_json": json.dumps([file_id] if is_found else []),
                "evidence_row_n": 1 if is_found else 0,
                "parser_ids_json": json.dumps(["html_links_text_v1"] if is_found else []),
                "error": "",
                "match_note": "",
            }
        )
    attempts = pd.DataFrame(attempts, columns=ATTEMPT_COLUMNS)
    files = pd.DataFrame(
        [
            {
                "contract_version": CONTRACT_VERSION,
                "file_id": file_id,
                "source_id": found["source_id"],
                "parse_group_id": found["parse_group_id"],
                "shard_id": found["shard_id"],
                "partition_hostname": found["partition_hostname"],
                "request_url": found["exact_source_url"],
                "final_url": found["exact_source_url"],
                "request_hostname": found["partition_hostname"],
                "final_hostname": found["partition_hostname"],
                "local_path": local_path,
                "file_type": "html",
                "content_type": "text/html",
                "http_status": 200,
                "bytes": len(content),
                "sha256": digest,
                "retrieved_at_utc": "2026-08-25T12:00:00Z",
                "parser_id": "html_links_text_v1",
                "parser_version": "1",
            }
        ],
        columns=FILE_COLUMNS,
    )
    evidence = pd.DataFrame(
        [
            {
                "contract_version": CONTRACT_VERSION,
                "evidence_id": "synthetic-evidence-1",
                "attempt_key": found["attempt_key"],
                "target_id": found["target_id"],
                "source_id": found["source_id"],
                "parse_group_id": found["parse_group_id"],
                "shard_id": found["shard_id"],
                "candidate_year_id": found_candidate["candidate_year_id"],
                "contest": found["contest"],
                "raw_url": "https://Campaign.Example/Mayor",
                "exact_normalized_url": "https://Campaign.Example/Mayor",
                "site_root_url": "https://Campaign.Example/",
                "registrable_domain": "campaign.example",
                "url_type": "custom_domain",
                "source_url": found["exact_source_url"],
                "source_date": "2013-11-01",
                "source_file_id": file_id,
                "source_file_sha256": digest,
                "source_snapshot_timestamp": "",
                "evidence_text": "Campaign site",
                "expected_website_field": found["expected_website_field"],
                "parser_id": "html_links_text_v1",
                "parser_version": "1",
                "identity_match": "pending_review",
                "temporal_match": "pending_review",
                "evidence_grade": "",
                "review_status": "pending",
            }
        ],
        columns=EVIDENCE_COLUMNS,
    )
    for shard, root in roots.items():
        attempts.loc[attempts["shard_id"].eq(shard)].to_csv(root / "attempts.csv", index=False)
        files.loc[files["shard_id"].eq(shard)].to_csv(root / "downloaded_files.csv", index=False)
        evidence.loc[evidence["shard_id"].eq(shard)].to_csv(root / "evidence.csv", index=False)
    return roots, attempts, files, evidence


def test_frozen_readiness_contract_is_complete_and_deterministically_partitioned() -> None:
    results = _real_contract()
    targets = results["targets"]
    candidates = results["candidate_targets"]
    matrix = results["parser_matrix"]
    summary = results["summary"]

    assert targets.columns.tolist() == TARGET_COLUMNS
    assert candidates.columns.tolist() == CANDIDATE_TARGET_COLUMNS
    assert len(targets) == 47
    assert len(candidates) == 264
    assert candidates["candidate_year_id"].nunique() > 69
    assert candidates.groupby("target_id").size().to_dict() == (
        targets.set_index("target_id")["unresolved_candidate_n"].astype(int).to_dict()
    )
    assert targets["target_id"].is_unique and targets["attempt_key"].is_unique
    assert targets["expected_website_field"].str.strip().ne("").all()
    assert targets.groupby("partition_hostname")["shard_id"].nunique().max() == 1
    assert summary["targets_by_shard"] == {"0": 25, "1": 22}
    assert summary["partition_hosts_by_shard"] == {"0": 4, "1": 6}
    for row in targets.itertuples(index=False):
        digest = hashlib.sha256(row.partition_hostname.encode("utf-8")).hexdigest()
        assert row.hostname_sha256 == digest
        assert row.shard_id == int(digest, 16) % 2
        allowed_types = json.loads(row.allowed_file_types_json)
        assert set(allowed_types) <= set(
            matrix.loc[matrix["source_type"].eq(row.source_type), "file_type"]
        )
    pdf_rows = matrix.loc[matrix["file_type"].eq("pdf")]
    assert pdf_rows["capabilities_json"].str.contains("pdf_hyperlink_annotations").all()


def test_offline_post_shard_directory_audit_accepts_exact_interface(tmp_path: Path) -> None:
    results = _real_contract()
    roots, _, _, _ = _valid_outputs(results, tmp_path)
    report = audit_post_shard_directories(
        results["targets"], results["candidate_targets"], results["parser_matrix"], roots
    )
    assert report == {
        "schema_version": "municipal-curated-post-shard-audit-v1",
        "verdict": "pass",
        "expected_attempts": 47,
        "terminal_attempts": 47,
        "downloaded_files": 1,
        "evidence_rows": 1,
        "found_targets": 1,
        "partition_hosts": 10,
        "shards": [0, 1],
    }


@pytest.mark.parametrize("failure", ["duplicate", "missing", "unexpected"])
def test_post_shard_audit_rejects_bad_attempt_keys(tmp_path: Path, failure: str) -> None:
    results = _real_contract()
    _, attempts, files, evidence = _valid_outputs(results, tmp_path)
    if failure == "duplicate":
        attempts = pd.concat([attempts, attempts.iloc[[0]]], ignore_index=True)
    elif failure == "missing":
        attempts = attempts.iloc[1:].copy()
    else:
        attempts.loc[0, "target_id"] = "unexpected-target"
    with pytest.raises(AcquisitionContractError):
        audit_post_shard_frames(
            results["targets"], results["candidate_targets"], results["parser_matrix"],
            attempts, files, evidence,
        )


@pytest.mark.parametrize("failure", ["missing_hash", "overlapping_file", "crossed_host", "unsupported_parser"])
def test_post_shard_audit_rejects_invalid_file_outputs(tmp_path: Path, failure: str) -> None:
    results = _real_contract()
    _, attempts, files, evidence = _valid_outputs(results, tmp_path)
    if failure == "missing_hash":
        files.loc[0, "sha256"] = ""
    elif failure == "overlapping_file":
        files = pd.concat([files, files.iloc[[0]]], ignore_index=True)
    elif failure == "crossed_host":
        files.loc[0, "shard_id"] = 1 - int(files.loc[0, "shard_id"])
    else:
        files.loc[0, "file_type"] = "docx"
        files.loc[0, "parser_id"] = "unsupported_parser"
    with pytest.raises(AcquisitionContractError):
        audit_post_shard_frames(
            results["targets"], results["candidate_targets"], results["parser_matrix"],
            attempts, files, evidence,
        )


def test_directory_audit_rejects_unexpected_output(tmp_path: Path) -> None:
    results = _real_contract()
    roots, _, _, _ = _valid_outputs(results, tmp_path)
    (roots[0] / "debug.log").write_text("not part of the contract", encoding="utf-8")
    with pytest.raises(AcquisitionContractError, match="unsupported outputs"):
        audit_post_shard_directories(
            results["targets"], results["candidate_targets"], results["parser_matrix"], roots
        )
