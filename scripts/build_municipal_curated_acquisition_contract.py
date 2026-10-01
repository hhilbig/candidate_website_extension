#!/usr/bin/env python3
"""Build and audit the offline two-shard contract for municipal/curated acquisition."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
from pathlib import Path
from urllib.parse import parse_qs, unquote, urlsplit

import pandas as pd


REPO_ROOT = Path(__file__).resolve().parents[1]
PRIORITY_DIR = REPO_ROOT / "data/municipal_discovery/screening_2010_2021/municipal_curated_priority"
DEFAULT_TARGETS = PRIORITY_DIR / "wave2_priority_targets.csv"
DEFAULT_CANDIDATES = PRIORITY_DIR / "wave2_candidate_targets.csv"
DEFAULT_SPECS = PRIORITY_DIR / "source_specs.json"
DEFAULT_PRIORITY_MANIFEST = PRIORITY_DIR / "output_manifest.json"
DEFAULT_OUTPUT_DIR = (
    REPO_ROOT
    / "data/municipal_discovery/screening_2010_2021/municipal_curated_acquisition/audit"
)

CONTRACT_VERSION = "municipal-curated-acquisition-contract-v1"
SHARD_COUNT = 2
TRANSPORT_HOSTS = {"web.archive.org"}
TERMINAL_STATUSES = {
    "found",
    "no_result",
    "source_unavailable",
    "request_error",
    "match_unresolved",
}
URL_TYPES = {
    "custom_domain",
    "candidate_subdomain",
    "shared_host_path",
    "fundraising",
    "social",
    "officeholder",
    "other_lead",
}

TARGET_COLUMNS = [
    "contract_version",
    "target_id",
    "attempt_key",
    "wave2_rank",
    "source_id",
    "source_name",
    "source_type",
    "geo_name",
    "state_abb",
    "year",
    "month",
    "contest",
    "parse_group_id",
    "partition_hostname",
    "hostname_sha256",
    "shard_id",
    "shard_count",
    "shard_label",
    "exact_source_url",
    "entry_urls_json",
    "allowed_request_hosts_json",
    "expected_website_field",
    "field_evidence_level",
    "field_evidence_score",
    "required_parser_capabilities_json",
    "allowed_file_types_json",
    "candidate_screen_n",
    "complete_contest_screen_n",
    "acquisition_candidate_ids_json",
    "unresolved_candidate_n",
]

CANDIDATE_TARGET_COLUMNS = [
    "contract_version",
    "target_id",
    "attempt_key",
    "source_id",
    "shard_id",
    "partition_hostname",
    "candidate_year_id",
    "full_name",
    "geo_name",
    "state_abb",
    "year",
    "month",
    "contest",
    "in_candidate_screen",
    "in_complete_contest_screen",
]

ATTEMPT_COLUMNS = [
    "contract_version",
    "attempt_key",
    "target_id",
    "source_id",
    "parse_group_id",
    "shard_id",
    "partition_hostname",
    "terminal_status",
    "attempted_at_utc",
    "retry_count",
    "http_status",
    "request_url",
    "final_url",
    "downloaded_file_n",
    "downloaded_file_ids_json",
    "evidence_row_n",
    "parser_ids_json",
    "error",
    "match_note",
]

FILE_COLUMNS = [
    "contract_version",
    "file_id",
    "source_id",
    "parse_group_id",
    "shard_id",
    "partition_hostname",
    "request_url",
    "final_url",
    "request_hostname",
    "final_hostname",
    "local_path",
    "file_type",
    "content_type",
    "http_status",
    "bytes",
    "sha256",
    "retrieved_at_utc",
    "parser_id",
    "parser_version",
]

EVIDENCE_COLUMNS = [
    "contract_version",
    "evidence_id",
    "attempt_key",
    "target_id",
    "source_id",
    "parse_group_id",
    "shard_id",
    "candidate_year_id",
    "contest",
    "raw_url",
    "exact_normalized_url",
    "site_root_url",
    "registrable_domain",
    "url_type",
    "source_url",
    "source_date",
    "source_file_id",
    "source_file_sha256",
    "source_snapshot_timestamp",
    "evidence_text",
    "expected_website_field",
    "parser_id",
    "parser_version",
    "identity_match",
    "temporal_match",
    "evidence_grade",
    "review_status",
]

PARSER_DEFINITIONS = {
    "html": {
        "parser_id": "html_links_text_v1",
        "capabilities": ["visible_text", "anchor_href", "url_context", "original_case"],
    },
    "pdf": {
        "parser_id": "pdf_text_annotations_v1",
        "capabilities": ["visible_text", "pdf_hyperlink_annotations", "url_context", "original_case"],
    },
    "csv": {
        "parser_id": "tabular_url_fields_v1",
        "capabilities": ["header_detection", "url_cell_values", "original_case"],
    },
    "xls": {
        "parser_id": "tabular_url_fields_v1",
        "capabilities": ["header_detection", "url_cell_values", "original_case"],
    },
    "xlsx": {
        "parser_id": "tabular_url_fields_v1",
        "capabilities": ["header_detection", "url_cell_values", "original_case"],
    },
    "json": {
        "parser_id": "json_url_fields_v1",
        "capabilities": ["recursive_url_values", "field_context", "original_case"],
    },
}

SOURCE_TYPE_FILES = {
    "curated_local_election_guide": {"required": ["html"], "allowed": ["html"]},
    "official_candidate_filing_page": {"required": ["html"], "allowed": ["html"]},
    "official_candidate_profile_guide": {"required": ["html"], "allowed": ["html", "json"]},
    "official_voter_guide_pdf": {"required": ["pdf"], "allowed": ["pdf", "html"]},
    "official_candidate_statement_archive": {
        "required": ["html", "pdf"],
        "allowed": ["html", "pdf", "csv", "xls", "xlsx", "json"],
    },
    "official_election_archive": {
        "required": ["html", "pdf"],
        "allowed": ["html", "pdf", "csv", "xls", "xlsx", "json"],
    },
    "official_sample_ballot_archive": {
        "required": ["html", "pdf"],
        "allowed": ["html", "pdf", "csv", "xls", "xlsx", "json"],
    },
}


class AcquisitionContractError(ValueError):
    """Raised when a frozen input or acquisition output violates the contract."""


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _text(value: object) -> str:
    if pd.isna(value):
        return ""
    return str(value).strip()


def _hostname(url: object) -> str:
    value = _text(url)
    parsed = urlsplit(value)
    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        raise AcquisitionContractError(f"Expected an exact HTTP(S) URL, found {value!r}")
    return parsed.hostname.casefold().rstrip(".")


def _as_bool(value: object) -> bool:
    if isinstance(value, bool):
        return value
    token = _text(value).casefold()
    if token in {"true", "1", "yes", "y"}:
        return True
    if token in {"false", "0", "no", "n", ""}:
        return False
    raise AcquisitionContractError(f"Unrecognized boolean value: {value!r}")


def _json_list(value: object, label: str) -> list[str]:
    if isinstance(value, list):
        result = value
    else:
        try:
            result = json.loads(_text(value))
        except json.JSONDecodeError as error:
            raise AcquisitionContractError(f"{label} is not valid JSON") from error
    if not isinstance(result, list) or any(not isinstance(item, str) for item in result):
        raise AcquisitionContractError(f"{label} must be a JSON array of strings")
    if len(result) != len(set(result)):
        raise AcquisitionContractError(f"{label} contains duplicate values")
    return result


def _attempt_key(target_id: str) -> str:
    digest = hashlib.sha256(target_id.encode("utf-8")).hexdigest()[:20]
    return f"municipal_attempt_{digest}"


def _hostname_partition(hostname: str) -> tuple[str, int]:
    digest = hashlib.sha256(hostname.encode("utf-8")).hexdigest()
    return digest, int(digest, 16) % SHARD_COUNT


def _require_exact_columns(frame: pd.DataFrame, columns: list[str], label: str) -> None:
    actual = frame.columns.tolist()
    if actual != columns:
        missing = [column for column in columns if column not in actual]
        unexpected = [column for column in actual if column not in columns]
        raise AcquisitionContractError(
            f"{label} schema mismatch; missing={missing}, unexpected={unexpected}, order={actual}"
        )


def _manifest_hash(manifest: dict, name: str) -> str:
    for item in manifest.get("outputs", []):
        if item.get("name") == name:
            return str(item["sha256"])
    raise AcquisitionContractError(f"Frozen manifest has no {name} output")


def build_parser_support_matrix(source_types: set[str]) -> pd.DataFrame:
    unsupported = sorted(source_types - set(SOURCE_TYPE_FILES))
    if unsupported:
        raise AcquisitionContractError(f"Unsupported source types: {unsupported}")
    rows: list[dict[str, object]] = []
    for source_type in sorted(source_types):
        config = SOURCE_TYPE_FILES[source_type]
        for file_type in config["allowed"]:
            parser = PARSER_DEFINITIONS.get(file_type)
            if parser is None:
                raise AcquisitionContractError(
                    f"No parser definition for {source_type}/{file_type}"
                )
            rows.append(
                {
                    "source_type": source_type,
                    "file_type": file_type,
                    "required_for_source_type": file_type in config["required"],
                    "parser_id": parser["parser_id"],
                    "capabilities_json": json.dumps(parser["capabilities"]),
                    "supported": True,
                }
            )
    return pd.DataFrame(rows).sort_values(["source_type", "file_type"]).reset_index(drop=True)


def build_contract(
    targets: pd.DataFrame,
    candidate_targets: pd.DataFrame,
    source_specs: dict,
    priority_manifest: dict,
    input_paths: dict[str, Path],
) -> dict[str, object]:
    expected_hashes = {
        "wave2_priority_targets": _manifest_hash(priority_manifest, "wave2_priority_targets"),
        "wave2_candidate_targets": _manifest_hash(priority_manifest, "wave2_candidate_targets"),
    }
    if _sha256(input_paths["targets"]) != expected_hashes["wave2_priority_targets"]:
        raise AcquisitionContractError("wave2 target file differs from its frozen manifest")
    if _sha256(input_paths["candidate_targets"]) != expected_hashes["wave2_candidate_targets"]:
        raise AcquisitionContractError("candidate-target file differs from its frozen manifest")
    expected_spec_hash = priority_manifest.get("inputs", {}).get("source_specs", {}).get("sha256")
    if not expected_spec_hash or _sha256(input_paths["source_specs"]) != expected_spec_hash:
        raise AcquisitionContractError("source specs differ from their frozen manifest")

    required_target_columns = {
        "wave2_rank", "target_id", "source_id", "source_name", "source_type",
        "disposition", "geo_name", "state_abb", "year", "month", "contest",
        "parse_group_id", "exact_source_url", "entry_urls_json", "expected_website_field",
        "field_evidence_level", "field_evidence_score", "candidate_screen_n",
        "complete_contest_screen_n", "candidate_screen_ids", "complete_contest_screen_ids",
        "unresolved_candidate_n", "screen_target",
    }
    missing = sorted(required_target_columns - set(targets.columns))
    if missing:
        raise AcquisitionContractError(f"Frozen targets are missing columns: {missing}")
    if len(targets) != 47 or targets["target_id"].duplicated().any():
        raise AcquisitionContractError("Frozen run-now target set must contain 47 unique targets")
    if not targets["disposition"].eq("run_now").all():
        raise AcquisitionContractError("Frozen target set contains a non-run-now row")
    if not targets["screen_target"].map(_as_bool).all():
        raise AcquisitionContractError("Frozen target set contains a non-screen target")
    ranks = pd.to_numeric(targets["wave2_rank"], errors="raise").astype(int)
    if sorted(ranks.tolist()) != list(range(1, 48)):
        raise AcquisitionContractError("wave2_rank must be exactly 1 through 47")

    spec_rows = source_specs.get("sources")
    if source_specs.get("schema_version") != "municipal-curated-priority-source-spec-v1":
        raise AcquisitionContractError("Unsupported source-spec schema")
    specs = {str(item["source_id"]): item for item in spec_rows if item["disposition"] == "run_now"}
    target_source_ids = set(targets["source_id"].astype(str))
    if target_source_ids != set(specs):
        raise AcquisitionContractError(
            f"Run-now source mismatch; targets_only={sorted(target_source_ids-set(specs))}, "
            f"specs_only={sorted(set(specs)-target_source_ids)}"
        )
    source_types = set(targets["source_type"].astype(str))
    parser_matrix = build_parser_support_matrix(source_types)

    candidate_required = {
        "target_id", "source_id", "candidate_year_id", "full_name", "geo_name",
        "state_abb", "year", "month", "contest", "in_candidate_screen",
        "in_complete_contest_screen",
    }
    candidate_missing = sorted(candidate_required - set(candidate_targets.columns))
    if candidate_missing:
        raise AcquisitionContractError(f"Candidate targets are missing columns: {candidate_missing}")
    if candidate_targets.duplicated(["target_id", "candidate_year_id"]).any():
        raise AcquisitionContractError("Candidate targets contain duplicate target/candidate keys")
    if not set(candidate_targets["target_id"].astype(str)) <= set(targets["target_id"].astype(str)):
        raise AcquisitionContractError("Candidate targets contain unexpected target IDs")

    candidate_ids_by_target = {
        target_id: sorted(group["candidate_year_id"].astype(str).unique().tolist())
        for target_id, group in candidate_targets.groupby("target_id")
    }
    contract_rows: list[dict[str, object]] = []
    host_owner_rows: list[dict[str, object]] = []
    seen_allowed_hosts: dict[str, int] = {}
    for row in targets.sort_values("wave2_rank").itertuples(index=False):
        source_id = str(row.source_id)
        spec = specs[source_id]
        if _text(row.expected_website_field) != _text(spec["expected_website_field"]):
            raise AcquisitionContractError(f"Expected website field drift for {source_id}")
        if not _text(row.expected_website_field) or int(row.field_evidence_score) <= 0:
            raise AcquisitionContractError(f"Target lacks website-field evidence: {row.target_id}")
        partition_hostname = _hostname(row.exact_source_url)
        digest, shard_id = _hostname_partition(partition_hostname)
        entry_urls = _json_list(row.entry_urls_json, f"{row.target_id}.entry_urls_json")
        allowed_hosts = sorted({_hostname(url) for url in entry_urls} | {partition_hostname})
        for allowed_hostname in allowed_hosts:
            if allowed_hostname in TRANSPORT_HOSTS:
                continue
            previous = seen_allowed_hosts.setdefault(allowed_hostname, shard_id)
            if previous != shard_id:
                raise AcquisitionContractError(
                    f"Direct host crosses shards: {allowed_hostname} ({previous}, {shard_id})"
                )
            host_owner_rows.append(
                {
                    "allowed_request_hostname": allowed_hostname,
                    "partition_hostname": partition_hostname,
                    "hostname_sha256": digest,
                    "source_id": source_id,
                    "shard_id": shard_id,
                    "shard_count": SHARD_COUNT,
                    "transport_host_exempt": allowed_hostname in TRANSPORT_HOSTS,
                }
            )
        source_config = SOURCE_TYPE_FILES[str(row.source_type)]
        required_capabilities = sorted(
            {
                capability
                for file_type in source_config["required"]
                for capability in PARSER_DEFINITIONS[file_type]["capabilities"]
            }
            | {"candidate_name_matching", "exact_url_preservation", "source_provenance"}
        )
        candidate_ids = candidate_ids_by_target.get(str(row.target_id), [])
        declared_ids = sorted(
            set(filter(None, _text(row.candidate_screen_ids).split(";")))
            | set(filter(None, _text(row.complete_contest_screen_ids).split(";")))
        )
        candidate_group = candidate_targets.loc[
            candidate_targets["target_id"].astype(str).eq(str(row.target_id))
        ]
        sampled_ids = sorted(candidate_group.loc[
            candidate_group["in_candidate_screen"].map(_as_bool)
            | candidate_group["in_complete_contest_screen"].map(_as_bool),
            "candidate_year_id",
        ].astype(str).unique())
        if sampled_ids != declared_ids:
            raise AcquisitionContractError(f"Sample candidate-target key drift for {row.target_id}")
        if len(candidate_ids) != int(row.unresolved_candidate_n):
            raise AcquisitionContractError(
                f"Full contest candidate-target count drift for {row.target_id}: "
                f"{len(candidate_ids)} != {int(row.unresolved_candidate_n)}"
            )
        contract_rows.append(
            {
                "contract_version": CONTRACT_VERSION,
                "target_id": str(row.target_id),
                "attempt_key": _attempt_key(str(row.target_id)),
                "wave2_rank": int(row.wave2_rank),
                "source_id": source_id,
                "source_name": str(row.source_name),
                "source_type": str(row.source_type),
                "geo_name": str(row.geo_name),
                "state_abb": str(row.state_abb),
                "year": int(row.year),
                "month": int(row.month),
                "contest": str(row.contest),
                "parse_group_id": str(row.parse_group_id),
                "partition_hostname": partition_hostname,
                "hostname_sha256": digest,
                "shard_id": shard_id,
                "shard_count": SHARD_COUNT,
                "shard_label": f"{shard_id}_of_{SHARD_COUNT}",
                "exact_source_url": str(row.exact_source_url),
                "entry_urls_json": json.dumps(entry_urls, ensure_ascii=False),
                "allowed_request_hosts_json": json.dumps(allowed_hosts),
                "expected_website_field": str(row.expected_website_field),
                "field_evidence_level": str(row.field_evidence_level),
                "field_evidence_score": int(row.field_evidence_score),
                "required_parser_capabilities_json": json.dumps(required_capabilities),
                "allowed_file_types_json": json.dumps(source_config["allowed"]),
                "candidate_screen_n": int(row.candidate_screen_n),
                "complete_contest_screen_n": int(row.complete_contest_screen_n),
                "acquisition_candidate_ids_json": json.dumps(candidate_ids),
                "unresolved_candidate_n": int(row.unresolved_candidate_n),
            }
        )
    contract = pd.DataFrame(contract_rows, columns=TARGET_COLUMNS)
    _require_exact_columns(contract, TARGET_COLUMNS, "target contract")
    if contract.groupby("partition_hostname")["shard_id"].nunique().max() != 1:
        raise AcquisitionContractError("A partition hostname crosses shards")
    if set(contract["shard_id"]) != {0, 1}:
        raise AcquisitionContractError("Both shards must receive at least one target")

    host_owners = (
        pd.DataFrame(host_owner_rows)
        .drop_duplicates(["allowed_request_hostname", "source_id", "shard_id"])
        .sort_values(["shard_id", "allowed_request_hostname", "source_id"])
        .reset_index(drop=True)
    )
    direct_host_shards = host_owners.loc[~host_owners["transport_host_exempt"]].groupby(
        "allowed_request_hostname"
    )["shard_id"].nunique()
    if len(direct_host_shards) and int(direct_host_shards.max()) != 1:
        raise AcquisitionContractError("An allowed direct request host crosses shards")

    candidate_contract = candidate_targets.merge(
        contract[
            ["target_id", "attempt_key", "source_id", "shard_id", "partition_hostname"]
        ],
        on=["target_id", "source_id"],
        how="left",
        validate="many_to_one",
    )
    if candidate_contract["shard_id"].isna().any():
        raise AcquisitionContractError("Candidate target failed to map to a contract target")
    candidate_contract.insert(0, "contract_version", CONTRACT_VERSION)
    candidate_contract = candidate_contract[CANDIDATE_TARGET_COLUMNS].sort_values(
        ["shard_id", "target_id", "candidate_year_id"]
    ).reset_index(drop=True)
    _require_exact_columns(candidate_contract, CANDIDATE_TARGET_COLUMNS, "candidate contract")

    summary = {
        "schema_version": "municipal-curated-acquisition-readiness-v1",
        "verdict": "pass",
        "contract_version": CONTRACT_VERSION,
        "target_rows": int(len(contract)),
        "candidate_source_rows": int(len(candidate_contract)),
        "unique_acquisition_candidates": int(candidate_contract["candidate_year_id"].nunique()),
        "run_now_sources": int(contract["source_id"].nunique()),
        "partition_hostnames": int(contract["partition_hostname"].nunique()),
        "allowed_direct_hosts": int(
            host_owners.loc[~host_owners["transport_host_exempt"], "allowed_request_hostname"].nunique()
        ),
        "targets_by_shard": {
            str(shard_id): int(count)
            for shard_id, count in contract["shard_id"].value_counts().sort_index().items()
        },
        "partition_hosts_by_shard": {
            str(shard_id): int(count)
            for shard_id, count in contract.groupby("shard_id")["partition_hostname"].nunique().items()
        },
        "candidate_source_rows_by_shard": {
            str(shard_id): int(count)
            for shard_id, count in candidate_contract["shard_id"].value_counts().sort_index().items()
        },
        "source_types": sorted(source_types),
        "parser_matrix_rows": int(len(parser_matrix)),
        "targets_with_expected_website_field": int(contract["expected_website_field"].ne("").sum()),
        "target_schema_columns": TARGET_COLUMNS,
        "attempt_schema_columns": ATTEMPT_COLUMNS,
        "file_schema_columns": FILE_COLUMNS,
        "evidence_schema_columns": EVIDENCE_COLUMNS,
        "terminal_statuses": sorted(TERMINAL_STATUSES),
        "url_types": sorted(URL_TYPES),
        "partition_algorithm": "lowercase original source hostname; SHA-256 UTF-8 digest interpreted as an integer modulo 2",
        "wayback_transport_rule": "web.archive.org is a transport wrapper; partition by the unwrapped original source hostname",
    }
    return {
        "targets": contract,
        "candidate_targets": candidate_contract,
        "host_owners": host_owners,
        "parser_matrix": parser_matrix,
        "summary": summary,
    }


def _transport_original_hostname(url: str) -> str | None:
    parsed = urlsplit(url)
    hostname = (parsed.hostname or "").casefold().rstrip(".")
    if hostname not in TRANSPORT_HOSTS:
        return hostname or None
    path_match = re.match(r"^/web/[^/]+/(https?://.+)$", parsed.path)
    if path_match:
        return _hostname(unquote(path_match.group(1)))
    query_url = parse_qs(parsed.query).get("url", [])
    if query_url:
        value = query_url[0]
        if not value.startswith(("http://", "https://")):
            value = "https://" + value.lstrip("/")
        return _hostname(value)
    return None


def _validate_host_for_source(
    url: str,
    declared_hostname: str,
    allowed_hosts: set[str],
    label: str,
) -> None:
    actual_hostname = _hostname(url)
    if actual_hostname != declared_hostname:
        raise AcquisitionContractError(
            f"{label} hostname mismatch: declared={declared_hostname}, actual={actual_hostname}"
        )
    if actual_hostname in TRANSPORT_HOSTS:
        original_hostname = _transport_original_hostname(url)
        if not original_hostname or original_hostname not in allowed_hosts:
            raise AcquisitionContractError(
                f"{label} Wayback URL does not unwrap to an allowed source host"
            )
    elif actual_hostname not in allowed_hosts:
        raise AcquisitionContractError(f"{label} uses unsupported host {actual_hostname}")


def _validate_relative_file_path(value: str) -> Path:
    path = Path(value)
    if path.is_absolute() or ".." in path.parts or not path.parts or path.parts[0] != "files":
        raise AcquisitionContractError(f"Downloaded local_path must be under files/: {value!r}")
    return path


def audit_post_shard_frames(
    contract: pd.DataFrame,
    candidate_contract: pd.DataFrame,
    parser_matrix: pd.DataFrame,
    attempts: pd.DataFrame,
    files: pd.DataFrame,
    evidence: pd.DataFrame,
    shard_roots: dict[int, Path] | None = None,
) -> dict[str, object]:
    """Fail closed unless combined shard outputs exactly satisfy the frozen contract."""
    _require_exact_columns(contract, TARGET_COLUMNS, "target contract")
    _require_exact_columns(candidate_contract, CANDIDATE_TARGET_COLUMNS, "candidate contract")
    _require_exact_columns(attempts, ATTEMPT_COLUMNS, "attempt output")
    _require_exact_columns(files, FILE_COLUMNS, "downloaded-file output")
    _require_exact_columns(evidence, EVIDENCE_COLUMNS, "evidence output")
    if contract["target_id"].duplicated().any() or contract["attempt_key"].duplicated().any():
        raise AcquisitionContractError("Contract has duplicate target or attempt keys")
    if attempts["target_id"].duplicated().any() or attempts["attempt_key"].duplicated().any():
        raise AcquisitionContractError("Duplicate terminal attempt key")
    expected_targets = set(contract["target_id"].astype(str))
    actual_targets = set(attempts["target_id"].astype(str))
    if actual_targets != expected_targets:
        raise AcquisitionContractError(
            f"Terminal attempt key mismatch; missing={sorted(expected_targets-actual_targets)}, "
            f"unexpected={sorted(actual_targets-expected_targets)}"
        )
    expected_attempts = set(contract["attempt_key"].astype(str))
    actual_attempts = set(attempts["attempt_key"].astype(str))
    if actual_attempts != expected_attempts:
        raise AcquisitionContractError("Attempt-key values differ from the contract")

    contract_lookup = contract.set_index("target_id")
    attempt_lookup = attempts.set_index("target_id")
    for target_id, row in attempt_lookup.iterrows():
        expected = contract_lookup.loc[target_id]
        for column in [
            "contract_version", "attempt_key", "source_id", "parse_group_id",
            "shard_id", "partition_hostname",
        ]:
            if _text(row[column]) != _text(expected[column]):
                raise AcquisitionContractError(f"Attempt {target_id} has incorrect {column}")
        if _text(row["terminal_status"]) not in TERMINAL_STATUSES:
            raise AcquisitionContractError(f"Attempt {target_id} has invalid terminal_status")
        if not _text(row["attempted_at_utc"]):
            raise AcquisitionContractError(f"Attempt {target_id} lacks attempted_at_utc")
        retry_count = int(row["retry_count"])
        if retry_count < 0:
            raise AcquisitionContractError(f"Attempt {target_id} has negative retry_count")
        file_ids = _json_list(row["downloaded_file_ids_json"], f"{target_id}.downloaded_file_ids_json")
        parser_ids = _json_list(row["parser_ids_json"], f"{target_id}.parser_ids_json")
        if int(row["downloaded_file_n"]) != len(file_ids):
            raise AcquisitionContractError(f"Attempt {target_id} downloaded_file_n disagrees with IDs")
        if _text(row["terminal_status"]) in {"request_error", "source_unavailable"} and not _text(row["error"]):
            raise AcquisitionContractError(f"Failed attempt {target_id} lacks an error")
        if _text(row["terminal_status"]) == "match_unresolved" and not _text(row["match_note"]):
            raise AcquisitionContractError(f"Unresolved match {target_id} lacks match_note")
        if file_ids and not parser_ids:
            raise AcquisitionContractError(f"Attempt {target_id} downloaded files but records no parser IDs")

    if files["file_id"].duplicated().any():
        raise AcquisitionContractError("Duplicate downloaded file_id")
    if files["local_path"].duplicated().any():
        raise AcquisitionContractError("Duplicate downloaded local_path")
    nonempty_hashes = files["sha256"].map(_text)
    if len(files) and (~nonempty_hashes.str.fullmatch(r"[0-9a-f]{64}")).any():
        raise AcquisitionContractError("Every downloaded file requires a lowercase SHA-256 hash")
    if len(files) and files["file_id"].astype(str).ne("sha256:" + nonempty_hashes).any():
        raise AcquisitionContractError("file_id must equal sha256:<sha256>")
    if len(files) and files["sha256"].duplicated().any():
        raise AcquisitionContractError("Downloaded content overlaps across file rows")
    if len(files) and files["request_url"].astype(str).duplicated().any():
        raise AcquisitionContractError("Downloaded request URLs overlap across file rows")

    source_contract = contract.groupby("source_id", as_index=False).agg(
        shard_id=("shard_id", "first"),
        partition_hostname=("partition_hostname", "first"),
        source_type=("source_type", "first"),
        allowed_request_hosts_json=("allowed_request_hosts_json", "first"),
    ).set_index("source_id")
    valid_parse_groups = set(contract["parse_group_id"].astype(str))
    support = {
        (str(row.source_type), str(row.file_type)): str(row.parser_id)
        for row in parser_matrix.loc[parser_matrix["supported"].map(_as_bool)].itertuples(index=False)
    }
    for row in files.itertuples(index=False):
        source_id = str(row.source_id)
        if source_id not in source_contract.index:
            raise AcquisitionContractError(f"Downloaded file has unexpected source_id {source_id}")
        expected = source_contract.loc[source_id]
        if int(row.shard_id) != int(expected.shard_id):
            raise AcquisitionContractError(f"Downloaded file for {source_id} is in the wrong shard")
        if str(row.partition_hostname) != str(expected.partition_hostname):
            raise AcquisitionContractError(f"Downloaded file for {source_id} has wrong partition hostname")
        if str(row.parse_group_id) not in valid_parse_groups:
            raise AcquisitionContractError(f"Downloaded file has unexpected parse_group_id {row.parse_group_id}")
        if str(row.contract_version) != CONTRACT_VERSION:
            raise AcquisitionContractError("Downloaded file has wrong contract_version")
        allowed_hosts = set(_json_list(expected.allowed_request_hosts_json, f"{source_id}.allowed_hosts"))
        _validate_host_for_source(
            str(row.request_url), str(row.request_hostname), allowed_hosts, f"{row.file_id}.request_url"
        )
        _validate_host_for_source(
            str(row.final_url), str(row.final_hostname), allowed_hosts, f"{row.file_id}.final_url"
        )
        expected_parser = support.get((str(expected.source_type), str(row.file_type)))
        if expected_parser is None or str(row.parser_id) != expected_parser:
            raise AcquisitionContractError(
                f"Unsupported parser output for {source_id}/{row.file_type}: {row.parser_id}"
            )
        if not _text(row.parser_version):
            raise AcquisitionContractError(f"Downloaded file {row.file_id} lacks parser_version")
        if int(row.bytes) <= 0 or not (200 <= int(row.http_status) < 400):
            raise AcquisitionContractError(f"Downloaded file {row.file_id} has invalid size/status")
        relative_path = _validate_relative_file_path(str(row.local_path))
        if shard_roots is not None:
            root = shard_roots.get(int(row.shard_id))
            if root is None:
                raise AcquisitionContractError(f"No shard root supplied for shard {row.shard_id}")
            path = root / relative_path
            if not path.is_file():
                raise AcquisitionContractError(f"Downloaded file is missing: {path}")
            if path.stat().st_size != int(row.bytes) or _sha256(path) != str(row.sha256):
                raise AcquisitionContractError(f"Downloaded file hash/size mismatch: {path}")

    host_shards: dict[str, set[int]] = {}
    for row in files.itertuples(index=False):
        host_shards.setdefault(str(row.partition_hostname), set()).add(int(row.shard_id))
    crossing = {host: shards for host, shards in host_shards.items() if len(shards) > 1}
    if crossing:
        raise AcquisitionContractError(f"Partition hosts cross shards: {crossing}")

    file_ids = set(files["file_id"].astype(str))
    referenced_file_ids: set[str] = set()
    for row in attempts.itertuples(index=False):
        ids = set(_json_list(row.downloaded_file_ids_json, f"{row.target_id}.downloaded_file_ids_json"))
        unexpected = ids - file_ids
        if unexpected:
            raise AcquisitionContractError(f"Attempt {row.target_id} references missing files: {sorted(unexpected)}")
        referenced_file_ids.update(ids)
    if referenced_file_ids != file_ids:
        raise AcquisitionContractError(
            f"Downloaded file manifest contains unreferenced files: {sorted(file_ids-referenced_file_ids)}"
        )

    if evidence["evidence_id"].duplicated().any():
        raise AcquisitionContractError("Duplicate evidence_id")
    expected_candidate_keys = set(
        zip(candidate_contract["target_id"].astype(str), candidate_contract["candidate_year_id"].astype(str))
    )
    evidence_counts = evidence.groupby("target_id").size().to_dict()
    file_hashes = files.set_index("file_id")["sha256"].to_dict() if len(files) else {}
    for row in evidence.itertuples(index=False):
        target_id = str(row.target_id)
        if target_id not in expected_targets:
            raise AcquisitionContractError(f"Evidence has unexpected target_id {target_id}")
        expected = contract_lookup.loc[target_id]
        for column in ["contract_version", "attempt_key", "source_id", "parse_group_id", "shard_id"]:
            if _text(getattr(row, column)) != _text(expected[column]):
                raise AcquisitionContractError(f"Evidence {row.evidence_id} has incorrect {column}")
        if (target_id, str(row.candidate_year_id)) not in expected_candidate_keys:
            raise AcquisitionContractError(f"Evidence {row.evidence_id} has an unexpected candidate key")
        if str(row.contest) != str(expected.contest):
            raise AcquisitionContractError(f"Evidence {row.evidence_id} has wrong contest")
        for column in [
            "raw_url", "exact_normalized_url", "site_root_url", "registrable_domain",
            "source_url", "source_file_id", "source_file_sha256", "evidence_text",
            "expected_website_field", "parser_id", "parser_version", "identity_match",
            "temporal_match", "review_status",
        ]:
            if not _text(getattr(row, column)):
                raise AcquisitionContractError(f"Evidence {row.evidence_id} lacks {column}")
        if str(row.url_type) not in URL_TYPES:
            raise AcquisitionContractError(f"Evidence {row.evidence_id} has invalid url_type")
        if str(row.source_file_id) not in file_ids:
            raise AcquisitionContractError(f"Evidence {row.evidence_id} references a missing source file")
        if str(row.source_file_sha256) != str(file_hashes[str(row.source_file_id)]):
            raise AcquisitionContractError(f"Evidence {row.evidence_id} source-file hash mismatch")
        if str(row.expected_website_field) != str(expected.expected_website_field):
            raise AcquisitionContractError(f"Evidence {row.evidence_id} expected-field drift")

    for row in attempts.itertuples(index=False):
        actual_n = int(evidence_counts.get(str(row.target_id), 0))
        if int(row.evidence_row_n) != actual_n:
            raise AcquisitionContractError(f"Attempt {row.target_id} evidence_row_n mismatch")
        if str(row.terminal_status) == "found" and actual_n == 0:
            raise AcquisitionContractError(f"Found attempt {row.target_id} has no evidence")
        if str(row.terminal_status) != "found" and actual_n != 0:
            raise AcquisitionContractError(f"Non-found attempt {row.target_id} has evidence rows")

    return {
        "schema_version": "municipal-curated-post-shard-audit-v1",
        "verdict": "pass",
        "expected_attempts": int(len(contract)),
        "terminal_attempts": int(len(attempts)),
        "downloaded_files": int(len(files)),
        "evidence_rows": int(len(evidence)),
        "found_targets": int(attempts["terminal_status"].eq("found").sum()),
        "partition_hosts": int(contract["partition_hostname"].nunique()),
        "shards": sorted(int(value) for value in contract["shard_id"].unique()),
    }


def audit_post_shard_directories(
    contract: pd.DataFrame,
    candidate_contract: pd.DataFrame,
    parser_matrix: pd.DataFrame,
    shard_roots: dict[int, Path],
) -> dict[str, object]:
    """Load exact shard interfaces, reject unexpected files, then audit combined outputs."""
    allowed_root_entries = {"attempts.csv", "downloaded_files.csv", "evidence.csv", "files"}
    frames: dict[str, list[pd.DataFrame]] = {"attempts": [], "files": [], "evidence": []}
    for shard_id in sorted(shard_roots):
        root = shard_roots[shard_id]
        if not root.is_dir():
            raise AcquisitionContractError(f"Shard root does not exist: {root}")
        actual_entries = {path.name for path in root.iterdir()}
        if actual_entries != allowed_root_entries:
            raise AcquisitionContractError(
                f"Shard {shard_id} has unsupported outputs; missing={sorted(allowed_root_entries-actual_entries)}, "
                f"unexpected={sorted(actual_entries-allowed_root_entries)}"
            )
        frames["attempts"].append(pd.read_csv(root / "attempts.csv", keep_default_na=False))
        frames["files"].append(pd.read_csv(root / "downloaded_files.csv", keep_default_na=False))
        frames["evidence"].append(pd.read_csv(root / "evidence.csv", keep_default_na=False))
    attempts = pd.concat(frames["attempts"], ignore_index=True)
    files = pd.concat(frames["files"], ignore_index=True)
    evidence = pd.concat(frames["evidence"], ignore_index=True)
    manifest_paths = {
        str((root / _validate_relative_file_path(str(row.local_path))).resolve())
        for row in files.itertuples(index=False)
        for shard, root in shard_roots.items()
        if int(row.shard_id) == int(shard)
    }
    actual_paths = {
        str(path.resolve())
        for root in shard_roots.values()
        for path in (root / "files").rglob("*")
        if path.is_file()
    }
    if manifest_paths != actual_paths:
        raise AcquisitionContractError(
            f"Raw file set differs from downloaded_files.csv; missing={sorted(manifest_paths-actual_paths)}, "
            f"unexpected={sorted(actual_paths-manifest_paths)}"
        )
    return audit_post_shard_frames(
        contract,
        candidate_contract,
        parser_matrix,
        attempts,
        files,
        evidence,
        shard_roots=shard_roots,
    )


def _render_report(summary: dict, contract: pd.DataFrame, parser_matrix: pd.DataFrame) -> str:
    source_type_rows = []
    for source_type, group in parser_matrix.groupby("source_type", sort=True):
        required = group.loc[group["required_for_source_type"], "file_type"].tolist()
        parsers = sorted(group["parser_id"].unique().tolist())
        source_type_rows.append(
            f"| {source_type} | {', '.join(required)} | {', '.join(parsers)} | Pass |"
        )
    return "\n".join(
        [
            "# Municipal and curated acquisition readiness audit",
            "",
            "The frozen Wave 2 queue is ready to split across two acquisition shards. All 47 targets have a stable shard, a supported parser path, a nonempty expected website field, and an exact terminal-output contract.",
            "",
            "## Why this audit was run",
            "",
            "This audit prevents two acquisition workers from fetching the same source host or returning incompatible attempt, file, and evidence tables. It runs only on frozen local files and makes no network request.",
            "",
            "## What is compared",
            "",
            "The audit compares the 47 frozen contest-source targets and 75 frozen source-candidate rows against the run-now source specs, the frozen priority manifest, the parser support matrix, and the required shard-output schemas.",
            "",
            "## Assumptions",
            "",
            "1. The original source hostname in `exact_source_url` owns the target, even when Wayback transports the request.",
            "2. SHA-256 of the lowercase original hostname, interpreted as an integer modulo two, defines shard 0 or shard 1.",
            "3. Every target must end with exactly one terminal attempt status: `found`, `no_result`, `source_unavailable`, `request_error`, or `match_unresolved`.",
            "4. Evidence can refer only to candidates in the frozen source-candidate contract. Other names found while parsing are outside this screen output.",
            "5. Downloaded files are immutable: each requires a content hash, byte count, supported file type, parser identity, and source URL.",
            "",
            "## Partition result",
            "",
            f"Shard 0 receives {summary['targets_by_shard']['0']} targets and {summary['partition_hosts_by_shard']['0']} original source hosts. Shard 1 receives {summary['targets_by_shard']['1']} targets and {summary['partition_hosts_by_shard']['1']} original source hosts. No direct source or entry host crosses shards.",
            "",
            "## Parser gate",
            "",
            "| Source type | Required file types | Supported parser IDs | Verdict |",
            "|---|---|---|---|",
            *source_type_rows,
            "",
            "Every PDF path requires both visible-text extraction and hyperlink-annotation extraction. HTML paths require anchor and visible-URL extraction. Tabular and JSON parsers preserve the submitted URL case.",
            "",
            "## Required shard interface",
            "",
            "Each shard directory must contain exactly `attempts.csv`, `downloaded_files.csv`, `evidence.csv`, and `files/`. The CSV column order is fixed in `output_schemas.json`; empty header templates are included in this audit directory.",
            "",
            "The offline post-shard audit fails on duplicate, missing, or unexpected attempt keys; missing or incorrect file hashes; unreferenced or overlapping files; original hosts assigned to both shards; unsupported file/parser combinations; evidence for candidates outside the frozen screen; and unexpected shard outputs.",
            "",
            "## Decision",
            "",
            "Acquisition can begin from the two frozen shard target files. Combine no shard output with project evidence until the offline post-shard audit returns `pass`.",
            "",
            "## Glossary",
            "",
            "- **Partition hostname:** the original source hostname that determines shard ownership.",
            "- **Transport host:** a wrapper such as `web.archive.org`; it does not determine shard ownership.",
            "- **Terminal attempt:** the single final status row for one target after any retries.",
            "- **Source-candidate row:** one candidate whom a source-target is allowed to match in the fixed screen.",
            "- **File manifest:** the immutable list of downloaded files, URLs, hashes, sizes, and parsers.",
            "",
        ]
    )


def write_contract_outputs(
    results: dict[str, object],
    output_dir: Path,
    input_paths: dict[str, Path],
) -> dict[str, object]:
    output_dir.mkdir(parents=True, exist_ok=True)
    contract: pd.DataFrame = results["targets"]
    candidates: pd.DataFrame = results["candidate_targets"]
    parser_matrix: pd.DataFrame = results["parser_matrix"]
    files_to_write: list[tuple[str, Path, pd.DataFrame | None, str | None]] = []
    files_to_write.extend(
        [
            ("acquisition_targets", output_dir / "acquisition_targets.csv", contract, None),
            ("acquisition_candidate_targets", output_dir / "acquisition_candidate_targets.csv", candidates, None),
            ("hostname_partition", output_dir / "hostname_partition.csv", results["host_owners"], None),
            ("parser_support_matrix", output_dir / "parser_support_matrix.csv", parser_matrix, None),
        ]
    )
    for shard_id in range(SHARD_COUNT):
        files_to_write.extend(
            [
                (
                    f"shard_{shard_id}_targets",
                    output_dir / f"shard_{shard_id}_targets.csv",
                    contract.loc[contract["shard_id"].eq(shard_id)].copy(),
                    None,
                ),
                (
                    f"shard_{shard_id}_candidate_targets",
                    output_dir / f"shard_{shard_id}_candidate_targets.csv",
                    candidates.loc[candidates["shard_id"].eq(shard_id)].copy(),
                    None,
                ),
            ]
        )
    schema_payload = {
        "schema_version": "municipal-curated-acquisition-output-schemas-v1",
        "contract_version": CONTRACT_VERSION,
        "target_columns": TARGET_COLUMNS,
        "candidate_target_columns": CANDIDATE_TARGET_COLUMNS,
        "attempt_columns": ATTEMPT_COLUMNS,
        "downloaded_file_columns": FILE_COLUMNS,
        "evidence_columns": EVIDENCE_COLUMNS,
        "terminal_statuses": sorted(TERMINAL_STATUSES),
        "url_types": sorted(URL_TYPES),
        "required_shard_entries": ["attempts.csv", "downloaded_files.csv", "evidence.csv", "files/"],
    }
    schemas_path = output_dir / "output_schemas.json"
    schemas_path.write_text(json.dumps(schema_payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    for name, columns in [
        ("attempt_template", ATTEMPT_COLUMNS),
        ("downloaded_file_template", FILE_COLUMNS),
        ("evidence_template", EVIDENCE_COLUMNS),
    ]:
        files_to_write.append((name, output_dir / f"{name}.csv", pd.DataFrame(columns=columns), None))
    summary_path = output_dir / "readiness_summary.json"
    summary_path.write_text(json.dumps(results["summary"], indent=2, sort_keys=True) + "\n", encoding="utf-8")
    report_path = output_dir / "readiness_report.md"
    report_path.write_text(
        _render_report(results["summary"], contract, parser_matrix), encoding="utf-8"
    )

    output_records: list[dict[str, object]] = []
    for name, path, frame, _ in files_to_write:
        assert frame is not None
        frame.to_csv(path, index=False)
        output_records.append(
            {
                "name": name,
                "path": path.name,
                "rows": int(len(frame)),
                "bytes": path.stat().st_size,
                "sha256": _sha256(path),
            }
        )
    for name, path in [
        ("output_schemas", schemas_path),
        ("readiness_summary", summary_path),
        ("readiness_report", report_path),
    ]:
        output_records.append(
            {
                "name": name,
                "path": path.name,
                "rows": None,
                "bytes": path.stat().st_size,
                "sha256": _sha256(path),
            }
        )
    manifest = {
        "schema_version": "municipal-curated-acquisition-contract-manifest-v1",
        "inputs": {
            name: {
                "path": str(path.relative_to(REPO_ROOT)),
                "bytes": path.stat().st_size,
                "sha256": _sha256(path),
            }
            for name, path in input_paths.items()
        },
        "outputs": output_records,
    }
    manifest_path = output_dir / "contract_manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return manifest


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--targets", type=Path, default=DEFAULT_TARGETS)
    parser.add_argument("--candidate-targets", type=Path, default=DEFAULT_CANDIDATES)
    parser.add_argument("--source-specs", type=Path, default=DEFAULT_SPECS)
    parser.add_argument("--priority-manifest", type=Path, default=DEFAULT_PRIORITY_MANIFEST)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    input_paths = {
        "targets": args.targets,
        "candidate_targets": args.candidate_targets,
        "source_specs": args.source_specs,
        "priority_manifest": args.priority_manifest,
    }
    results = build_contract(
        pd.read_csv(args.targets),
        pd.read_csv(args.candidate_targets),
        json.loads(args.source_specs.read_text(encoding="utf-8")),
        json.loads(args.priority_manifest.read_text(encoding="utf-8")),
        input_paths,
    )
    manifest = write_contract_outputs(results, args.output_dir, input_paths)
    print(json.dumps({"summary": results["summary"], "manifest": manifest}, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
