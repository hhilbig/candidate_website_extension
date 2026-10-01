#!/usr/bin/env python3
"""Build a review-aware claim table without changing legacy coverage outputs.

The existing combined evidence file remains the source for the published
legacy baseline.  This script copies those rows into a separate claim schema
and labels them as ungraded under the current A--D rules.  Adapter claims keep
their supplied grade and review fields.  A strict flag can become true only
when a claim has an explicit human-acceptance status, a true acceptance value,
grade A or B, matching identity/timing (when supplied), and an eligible URL
type.

Input files are always explicit.  The script does not scan directories or make
network requests.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Iterable, Sequence
from urllib.parse import urlsplit

import pandas as pd


REPO_ROOT = Path(__file__).resolve().parents[1]

import sys

if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.mayoral_discovery import canonical_http_url, hostname, site_root_url


SCHEMA_VERSION = "mayoral_url_claims_v1"
STRICT_GRADES = {"A", "B"}
HUMAN_ACCEPTED_STATUSES = {"accepted", "approved"}
REJECTED_STATUSES = {"rejected", "declined"}
CUSTOM_DOMAIN_TYPES = {"custom_domain", "candidate_owned_custom_domain"}
EXACT_URL_TYPES = CUSTOM_DOMAIN_TYPES | {
    "candidate_subdomain",
    "shared_host_path",
    "candidate_subdomain_or_shared_host_path",
}

LEGACY_SOURCE_BY_METHOD = {
    "ballotpedia_campaign_section_citation": "internet_archive_ballotpedia",
    "ballotpedia_election_page_campaign_section_citation": (
        "internet_archive_ballotpedia_election_pages"
    ),
    "seattle_official_campaign_contact_csv": "seattle_ethics_campaign_contacts",
    "nyc_cfb_candidate_supplied_campaign_website": "nyc_cfb_voter_guides",
    "sf_official_candidate_submitted_voter_pamphlet_statement": (
        "sf_department_elections_voter_pamphlets"
    ),
    "curated_election_guide_explicit_campaign_website": (
        "curated_explicit_election_guides"
    ),
    "curated_election_guide_explicit_official_campaign_website": (
        "curated_explicit_election_guides"
    ),
    "curated_general_election_guide_candidate_heading_link": (
        "curated_explicit_election_guides"
    ),
    "nyc_cfb_official_general_election_voter_guide_pdf": (
        "curated_explicit_election_guides"
    ),
}

LEGACY_PARSER_BY_SOURCE = {
    "internet_archive_ballotpedia": "ballotpedia_profile_archive_v1",
    "internet_archive_ballotpedia_election_pages": (
        "ballotpedia_election_page_archive_v1"
    ),
    "seattle_ethics_campaign_contacts": "seattle_campaign_contact_v1",
    "nyc_cfb_voter_guides": "nyc_cfb_voter_guide_v1",
    "sf_department_elections_voter_pamphlets": "curated_mayoral_urls_v1",
    "curated_explicit_election_guides": "curated_mayoral_urls_v1",
}

SHARED_HOSTS = {
    "blogspot.com",
    "digital.library.ucla.edu",
    "geocities.com",
    "google.com",
    "nationbuilder.com",
    "squarespace.com",
    "tumblr.com",
    "web.mac.com",
    "weebly.com",
    "wixsite.com",
    "wordpress.com",
}
SOCIAL_HOSTS = {
    "facebook.com",
    "instagram.com",
    "linkedin.com",
    "tiktok.com",
    "twitter.com",
    "x.com",
    "youtube.com",
}
FUNDRAISING_HOSTS = {
    "actblue.com",
    "anedot.com",
    "donorbox.org",
    "gofundme.com",
    "paypal.com",
    "winred.com",
}
COMMON_SECOND_LEVEL_SUFFIXES = {
    "ac.uk",
    "co.nz",
    "co.uk",
    "com.au",
    "com.br",
    "com.mx",
    "gov.uk",
    "net.au",
    "org.au",
    "org.uk",
}

CLAIM_COLUMNS = [
    "claim_id",
    "source_id",
    "claim_origin",
    "claim_schema_version",
    "parser_version",
    "source_file",
    "candidate_year_id",
    "candidate_contest_id",
    "contest_id",
    "ledb_candid",
    "full_name",
    "geo_name",
    "state_abb",
    "year",
    "month",
    "raw_url",
    "exact_normalized_url",
    "site_root_url",
    "registrable_domain",
    "url_type",
    "source_date",
    "source_profile_url",
    "source_snapshot_timestamp",
    "url_source",
    "evidence_text",
    "discovery_method",
    "identity_match",
    "temporal_match",
    "evidence_grade",
    "review_status",
    "acceptance",
    "human_accepted",
    "strict_custom_domain",
    "strict_exact_url",
    "strict_exclusion_reason",
    "legacy_validation_status",
]


class ClaimSchemaError(ValueError):
    """An evidence input violates the claim-table contract."""


def _display_path(path: Path) -> str:
    try:
        return path.resolve().relative_to(REPO_ROOT).as_posix()
    except ValueError:
        return str(path.resolve())


def _clean_text(value: object) -> str:
    if value is None or pd.isna(value):
        return ""
    return str(value).strip()


def _normalize_token(value: object) -> str:
    return re.sub(r"[^a-z0-9]+", "_", _clean_text(value).casefold()).strip("_")


def _stable_id(prefix: str, *values: object) -> str:
    payload = "\x1f".join(_clean_text(value) for value in values)
    digest = hashlib.sha256(payload.encode("utf-8")).hexdigest()[:24]
    return f"{prefix}_{digest}"


def _as_optional_bool(value: object) -> bool | None:
    if value is None or pd.isna(value) or _clean_text(value) == "":
        return None
    if isinstance(value, bool):
        return value
    token = _normalize_token(value)
    if token in {"1", "true", "t", "yes", "y", "accepted", "accept"}:
        return True
    if token in {"0", "false", "f", "no", "n", "rejected", "reject"}:
        return False
    raise ClaimSchemaError(f"Cannot interpret boolean value: {value!r}")


def _positive_match(value: object, *, missing_is_true: bool) -> bool:
    text = _clean_text(value)
    if not text:
        return missing_is_true
    optional = None
    try:
        optional = _as_optional_bool(value)
    except ClaimSchemaError:
        pass
    if optional is not None:
        return optional
    token = _normalize_token(value)
    negative_parts = {"mismatch", "unresolved", "unknown", "outside", "conflict"}
    if any(part in token.split("_") for part in negative_parts):
        return False
    return True


def registrable_domain(value: str) -> str:
    """Return a deterministic registrable-domain approximation."""
    host = hostname(value)
    if not host:
        return ""
    labels = host.split(".")
    if len(labels) <= 2:
        return host
    last_two = ".".join(labels[-2:])
    if last_two in COMMON_SECOND_LEVEL_SUFFIXES and len(labels) >= 3:
        return ".".join(labels[-3:])
    return last_two


def classify_url(value: str) -> str:
    """Classify legacy URLs while leaving adapter classifications intact."""
    exact = canonical_http_url(value)
    host = hostname(exact)
    registered = registrable_domain(exact)
    parsed = urlsplit(exact)
    if registered in SOCIAL_HOSTS:
        return "excluded_social_lead"
    if registered in FUNDRAISING_HOSTS:
        return "excluded_fundraising_lead"
    if host in SHARED_HOSTS or registered in SHARED_HOSTS:
        if host != registered or parsed.path not in {"", "/"}:
            return "candidate_subdomain_or_shared_host_path"
        return "shared_host_path"
    return "custom_domain"


def _source_date(row: pd.Series) -> str:
    supplied = _clean_text(row.get("source_date", ""))
    if supplied:
        try:
            return pd.Timestamp(supplied).date().isoformat()
        except (TypeError, ValueError):
            return supplied
    timestamp = re.sub(r"\.0$", "", _clean_text(row.get("source_snapshot_timestamp", "")))
    if re.fullmatch(r"\d{14}", timestamp):
        return datetime.strptime(timestamp[:8], "%Y%m%d").date().isoformat()
    return ""


def _contest_id(candidate_contest_id: str) -> str:
    return candidate_contest_id.rsplit("__", 1)[0] if "__" in candidate_contest_id else ""


def _optional_int(value: object) -> int | None:
    if value is None or pd.isna(value) or _clean_text(value) == "":
        return None
    return int(float(value))


def _universe_lookup(universe: pd.DataFrame) -> dict[str, dict[str, object]]:
    required = {
        "candidate_year_id",
        "candidate_contest_id",
        "ledb_candid",
        "full_name",
        "geo_name",
        "state_abb",
        "year",
        "month",
    }
    missing = sorted(required - set(universe.columns))
    if missing:
        raise ClaimSchemaError(f"Candidate universe is missing columns: {missing}")
    ids = universe["candidate_year_id"].astype("string").str.strip()
    if ids.isna().any() or ids.eq("").any():
        raise ClaimSchemaError("Candidate universe has missing candidate_year_id values")
    if ids.duplicated().any():
        raise ClaimSchemaError("Candidate universe has duplicate candidate_year_id values")
    frame = universe.copy()
    frame["candidate_year_id"] = ids
    return frame.set_index("candidate_year_id").to_dict("index")


def _context(row: pd.Series, lookup: dict[str, dict[str, object]]) -> dict[str, object]:
    candidate_id = _clean_text(row.get("candidate_year_id", ""))
    if not candidate_id or candidate_id not in lookup:
        raise ClaimSchemaError(
            f"Evidence candidate_year_id is absent from the universe: {candidate_id!r}"
        )
    context = lookup[candidate_id]
    for column in ("year", "month"):
        supplied = row.get(column, None)
        expected = context[column]
        if supplied is None or pd.isna(supplied) or _clean_text(supplied) == "":
            continue
        if expected is None or pd.isna(expected) or _clean_text(expected) == "":
            raise ClaimSchemaError(
                f"Evidence {candidate_id} supplies {column}={supplied}, but the universe is missing it"
            )
        if float(supplied) != float(expected):
            raise ClaimSchemaError(
                f"Evidence {candidate_id} has {column}={supplied}; universe has {expected}"
            )
    candidate_contest_id = _clean_text(context["candidate_contest_id"])
    supplied_contest = _clean_text(row.get("candidate_contest_id", ""))
    if supplied_contest and supplied_contest != candidate_contest_id:
        raise ClaimSchemaError(
            f"Evidence {candidate_id} has a candidate_contest_id that disagrees with the universe"
        )
    return {
        "candidate_year_id": candidate_id,
        "candidate_contest_id": candidate_contest_id,
        "contest_id": _clean_text(row.get("contest_id", "")) or _contest_id(candidate_contest_id),
        "ledb_candid": context["ledb_candid"],
        "full_name": _clean_text(context["full_name"]),
        "geo_name": _clean_text(context["geo_name"]),
        "state_abb": _clean_text(context["state_abb"]),
        "year": _optional_int(context["year"]),
        "month": _optional_int(context["month"]),
    }


def _strict_fields(
    *, origin: str, grade: str, review_status: str, acceptance: bool | None,
    url_type: str, identity_match: object, temporal_match: object,
) -> dict[str, object]:
    if origin == "legacy_baseline":
        return {
            "human_accepted": False,
            "strict_custom_domain": False,
            "strict_exact_url": False,
            "strict_exclusion_reason": "legacy_not_adjudicated_under_current_rules",
        }

    accepted_status = review_status in HUMAN_ACCEPTED_STATUSES
    rejected_status = review_status in REJECTED_STATUSES
    if accepted_status and acceptance is not True:
        raise ClaimSchemaError(
            "An accepted/approved adapter claim must have acceptance=true"
        )
    if acceptance is True and not accepted_status:
        raise ClaimSchemaError(
            "An adapter claim with acceptance=true must have review_status accepted or approved"
        )
    if rejected_status and acceptance is True:
        raise ClaimSchemaError("A rejected adapter claim cannot have acceptance=true")

    human_accepted = accepted_status and acceptance is True
    identity_ok = _positive_match(identity_match, missing_is_true=True)
    temporal_ok = _positive_match(temporal_match, missing_is_true=True)
    grade_ok = grade in STRICT_GRADES
    exact_type = url_type in EXACT_URL_TYPES
    strict_exact = human_accepted and grade_ok and identity_ok and temporal_ok and exact_type
    strict_custom = strict_exact and url_type in CUSTOM_DOMAIN_TYPES

    if not human_accepted:
        reason = "human_acceptance_required"
    elif not grade_ok:
        reason = "evidence_grade_not_strict"
    elif not identity_ok:
        reason = "identity_mismatch"
    elif not temporal_ok:
        reason = "temporal_mismatch"
    elif not exact_type:
        reason = "url_type_not_strict"
    else:
        reason = ""
    return {
        "human_accepted": human_accepted,
        "strict_custom_domain": strict_custom,
        "strict_exact_url": strict_exact,
        "strict_exclusion_reason": reason,
    }


def _legacy_claim(
    row: pd.Series, lookup: dict[str, dict[str, object]], source_file: str,
) -> dict[str, object]:
    method = _clean_text(row.get("discovery_method", ""))
    source_id = LEGACY_SOURCE_BY_METHOD.get(method)
    if source_id is None:
        raise ClaimSchemaError(
            f"No stable source_id mapping exists for legacy discovery_method {method!r}"
        )
    context = _context(row, lookup)
    raw_url = _clean_text(row.get("evidence_url", ""))
    exact = canonical_http_url(
        _clean_text(row.get("validated_campaign_url", "")) or raw_url
    )
    if not raw_url or not exact:
        raise ClaimSchemaError(
            f"Legacy evidence {context['candidate_year_id']} has no valid HTTP(S) URL"
        )
    parser_version = LEGACY_PARSER_BY_SOURCE[source_id]
    claim_id = _stable_id(
        "legacy_claim",
        SCHEMA_VERSION,
        source_id,
        context["candidate_year_id"],
        exact,
        row.get("source_profile_url", ""),
        row.get("source_snapshot_timestamp", ""),
        method,
        row.get("evidence_text", ""),
    )
    url_type = classify_url(exact)
    result = {
        **context,
        "claim_id": claim_id,
        "source_id": source_id,
        "claim_origin": "legacy_baseline",
        "claim_schema_version": SCHEMA_VERSION,
        "parser_version": parser_version,
        "source_file": source_file,
        "raw_url": raw_url,
        "exact_normalized_url": exact,
        "site_root_url": site_root_url(exact),
        "registrable_domain": registrable_domain(exact),
        "url_type": url_type,
        "source_date": _source_date(row),
        "source_profile_url": _clean_text(row.get("source_profile_url", "")),
        "source_snapshot_timestamp": _clean_text(row.get("source_snapshot_timestamp", "")),
        "url_source": _clean_text(row.get("url_source", "")),
        "evidence_text": _clean_text(row.get("evidence_text", "")),
        "discovery_method": method,
        "identity_match": "legacy_not_reassessed",
        "temporal_match": "legacy_not_reassessed",
        "evidence_grade": "legacy_ungraded",
        "review_status": "legacy_not_adjudicated_current_rules",
        "acceptance": None,
        "legacy_validation_status": _clean_text(row.get("validation_status", "")),
    }
    result.update(_strict_fields(
        origin="legacy_baseline",
        grade=result["evidence_grade"],
        review_status=result["review_status"],
        acceptance=None,
        url_type=url_type,
        identity_match=result["identity_match"],
        temporal_match=result["temporal_match"],
    ))
    return result


def _adapter_claim(
    row: pd.Series, lookup: dict[str, dict[str, object]], source_file: str,
) -> dict[str, object]:
    context = _context(row, lookup)
    source_id = _normalize_token(row.get("source_id", ""))
    parser_version = _clean_text(row.get("parser_version", ""))
    supplied_claim_id = _clean_text(row.get("claim_id", ""))
    if not source_id or not parser_version:
        raise ClaimSchemaError("Adapter claims require source_id and parser_version")
    raw_url = _clean_text(row.get("raw_url", "")) or _clean_text(row.get("evidence_url", ""))
    exact_source = (
        _clean_text(row.get("exact_normalized_url", ""))
        or _clean_text(row.get("validated_campaign_url", ""))
        or raw_url
    )
    exact = canonical_http_url(exact_source)
    if not raw_url or not exact:
        raise ClaimSchemaError(
            f"Adapter evidence {context['candidate_year_id']} has no valid HTTP(S) URL"
        )
    claim_id = supplied_claim_id or _stable_id(
        "adapter_claim",
        SCHEMA_VERSION,
        source_id,
        context["candidate_year_id"],
        exact,
        row.get("source_profile_url", ""),
        row.get("source_record_id", ""),
        row.get("source_snapshot_timestamp", ""),
        row.get("discovery_method", row.get("extraction_method", "")),
        row.get("evidence_text", ""),
    )
    url_type = _normalize_token(row.get("url_type", "")) or classify_url(exact)
    grade = _clean_text(row.get("evidence_grade", "")).upper()
    if grade not in {"A", "B", "C", "D"}:
        raise ClaimSchemaError(
            f"Adapter claim {claim_id} has invalid evidence_grade {grade!r}"
        )
    review_status = _normalize_token(row.get("review_status", "")) or "pending"
    acceptance = _as_optional_bool(row.get("acceptance", None))
    identity_match = row.get("identity_match", "")
    temporal_match = row.get("temporal_match", "")
    result = {
        **context,
        "claim_id": claim_id,
        "source_id": source_id,
        "claim_origin": "adapter",
        "claim_schema_version": SCHEMA_VERSION,
        "parser_version": parser_version,
        "source_file": source_file,
        "raw_url": raw_url,
        "exact_normalized_url": exact,
        "site_root_url": site_root_url(exact),
        "registrable_domain": registrable_domain(exact),
        "url_type": url_type,
        "source_date": _source_date(row),
        "source_profile_url": _clean_text(row.get("source_profile_url", "")),
        "source_snapshot_timestamp": _clean_text(row.get("source_snapshot_timestamp", "")),
        "url_source": _clean_text(row.get("url_source", row.get("source_name", ""))),
        "evidence_text": _clean_text(row.get("evidence_text", "")),
        "discovery_method": _clean_text(
            row.get("discovery_method", row.get("extraction_method", ""))
        ),
        "identity_match": _clean_text(identity_match),
        "temporal_match": _clean_text(temporal_match),
        "evidence_grade": grade,
        "review_status": review_status,
        "acceptance": acceptance,
        "legacy_validation_status": "",
    }
    result.update(_strict_fields(
        origin="adapter",
        grade=grade,
        review_status=review_status,
        acceptance=acceptance,
        url_type=url_type,
        identity_match=identity_match,
        temporal_match=temporal_match,
    ))
    return result


def build_claim_table(
    universe: pd.DataFrame,
    legacy_evidence: pd.DataFrame,
    adapter_evidence: Sequence[tuple[str, pd.DataFrame]] = (),
    *,
    legacy_source_file: str = "legacy_evidence",
) -> tuple[pd.DataFrame, dict[str, object]]:
    """Standardize legacy and adapter evidence into one claim-level table."""
    lookup = _universe_lookup(universe)
    required_legacy = {"candidate_year_id", "evidence_url", "discovery_method"}
    missing_legacy = sorted(required_legacy - set(legacy_evidence.columns))
    if missing_legacy:
        raise ClaimSchemaError(f"Legacy evidence is missing columns: {missing_legacy}")

    records = [
        _legacy_claim(row, lookup, legacy_source_file)
        for _, row in legacy_evidence.iterrows()
    ]
    input_counts = {legacy_source_file: int(len(legacy_evidence))}
    adapter_rows = 0
    for source_file, frame in adapter_evidence:
        if source_file in input_counts:
            raise ClaimSchemaError(f"Evidence input was supplied twice: {source_file}")
        input_counts[source_file] = int(len(frame))
        adapter_rows += len(frame)
        records.extend(_adapter_claim(row, lookup, source_file) for _, row in frame.iterrows())

    result = pd.DataFrame.from_records(records, columns=CLAIM_COLUMNS)
    if result.empty:
        result = pd.DataFrame(columns=CLAIM_COLUMNS)
    if result["claim_id"].astype("string").duplicated().any():
        duplicates = result.loc[
            result["claim_id"].astype("string").duplicated(False),
            ["claim_id", "source_id", "source_file"],
        ]
        raise ClaimSchemaError(
            "Duplicate claim_id values would collapse independent claims: "
            + duplicates.head(5).to_dict("records").__repr__()
        )
    exact_key = ["claim_id", "source_id", "exact_normalized_url"]
    if result.duplicated(exact_key).any():
        raise ClaimSchemaError("Claim table has duplicate exact evidence keys")

    result = result.sort_values(
        ["year", "state_abb", "geo_name", "full_name", "source_id", "claim_id"]
    ).reset_index(drop=True)
    expected_ids = {record["claim_id"] for record in records}
    observed_ids = set(result["claim_id"].astype(str))
    if observed_ids != expected_ids or len(result) != len(records):
        raise ClaimSchemaError("Output claim IDs do not agree exactly with standardized inputs")

    audit = {
        "claim_schema_version": SCHEMA_VERSION,
        "input_files": input_counts,
        "input_claim_rows": int(sum(input_counts.values())),
        "legacy_claim_rows": int(len(legacy_evidence)),
        "adapter_claim_rows": int(adapter_rows),
        "output_claim_rows": int(len(result)),
        "unique_claim_ids": int(result["claim_id"].nunique()),
        "exact_claim_id_agreement": True,
        "strict_custom_domain_claims": int(result["strict_custom_domain"].sum()),
        "strict_exact_url_claims": int(result["strict_exact_url"].sum()),
        "claims_by_origin": {
            str(key): int(value)
            for key, value in result["claim_origin"].value_counts().sort_index().items()
        },
        "claims_by_grade": {
            str(key): int(value)
            for key, value in result["evidence_grade"].value_counts().sort_index().items()
        },
        "claims_by_review_status": {
            str(key): int(value)
            for key, value in result["review_status"].value_counts().sort_index().items()
        },
    }
    return result, audit


def _atomic_csv(frame: pd.DataFrame, output: Path) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        mode="w", encoding="utf-8", suffix=".csv", dir=output.parent, delete=False
    ) as handle:
        temporary = Path(handle.name)
        frame.to_csv(handle, index=False)
    temporary.chmod(0o644)
    os.replace(temporary, output)


def write_claim_table(
    claims: pd.DataFrame,
    audit: dict[str, object],
    output: Path,
    audit_output: Path,
) -> dict[str, object]:
    """Atomically replace outputs and report rows pruned from a prior build."""
    previous_ids: set[str] = set()
    if output.is_file():
        previous = pd.read_csv(output, usecols=["claim_id"], dtype="string")
        previous_ids = set(previous["claim_id"].dropna().astype(str))
    current_ids = set(claims["claim_id"].astype(str))
    final_audit = dict(audit)
    final_audit.update({
        "previous_claim_rows": int(len(previous_ids)),
        "stale_claims_removed": int(len(previous_ids - current_ids)),
        "new_claims_added": int(len(current_ids - previous_ids)),
        "output_file": _display_path(output),
    })
    _atomic_csv(claims, output)
    audit_output.parent.mkdir(parents=True, exist_ok=True)
    audit_output.write_text(
        json.dumps(final_audit, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    return final_audit


def _resolve(path: str) -> Path:
    value = Path(path)
    return value if value.is_absolute() else REPO_ROOT / value


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--universe",
        default="data/municipal_discovery/final/mayoral_candidate_universe.csv",
    )
    parser.add_argument(
        "--legacy-evidence",
        default="data/municipal_discovery/final/mayoral_url_evidence.csv",
    )
    parser.add_argument(
        "--adapter-evidence",
        action="append",
        default=[],
        help="Explicit adapter evidence CSV; repeat for multiple files.",
    )
    parser.add_argument(
        "--output",
        default="data/municipal_discovery/final/mayoral_url_claims.csv",
    )
    parser.add_argument(
        "--audit-output",
        default=(
            "quality_reports/mayoral_url_discovery_2026-08-24/"
            "mayoral_url_claims_audit.json"
        ),
    )
    args = parser.parse_args()

    universe_path = _resolve(args.universe)
    legacy_path = _resolve(args.legacy_evidence)
    adapter_paths = [_resolve(value) for value in args.adapter_evidence]
    if len(set(adapter_paths)) != len(adapter_paths):
        raise ClaimSchemaError("The same adapter evidence path was supplied more than once")
    missing = [
        str(path) for path in [universe_path, legacy_path, *adapter_paths]
        if not path.is_file()
    ]
    if missing:
        raise ClaimSchemaError(f"Evidence inputs are missing: {missing}")

    claims, audit = build_claim_table(
        pd.read_csv(universe_path, low_memory=False),
        pd.read_csv(legacy_path, low_memory=False),
        [(_display_path(path), pd.read_csv(path, low_memory=False)) for path in adapter_paths],
        legacy_source_file=_display_path(legacy_path),
    )
    final_audit = write_claim_table(
        claims, audit, _resolve(args.output), _resolve(args.audit_output)
    )
    print(json.dumps(final_audit, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
