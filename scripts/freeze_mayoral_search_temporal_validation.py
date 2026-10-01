#!/usr/bin/env python3
"""Freeze high-precision search leads for temporal ownership validation."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import tempfile
from pathlib import Path
from urllib.parse import urlsplit

import pandas as pd

from scripts.run_mayoral_search_source_expansion_2010_2021 import (
    URL_LEAD_COLUMNS,
    _candidate_aliases,
    _text,
)


REPO_ROOT = Path(__file__).resolve().parents[1]
SEARCH_DIR = REPO_ROOT / "data/municipal_discovery/screening_2010_2021/search_source_expansion"
DEFAULT_LEADS = SEARCH_DIR / "url_leads.csv"
DEFAULT_LINKS = SEARCH_DIR / "page_links.csv"
DEFAULT_OUTPUT_DIR = SEARCH_DIR / "temporal_validation/frozen"
FREEZE_CONTRACT_VERSION = "search_lead_wayback_available_freeze_v1"
VALIDATION_METHOD_VERSION = "search_lead_wayback_available_content_v1"
MANIFEST_VERSION = "search-lead-wayback-available-manifest-v1"
VALIDATION_SHARD_COUNT = 2
COVERAGE_TYPES = {"custom_domain", "candidate_subdomain", "shared_host_path"}
CAMPAIGN_HOST_MARKERS = ("mayor", "elect", "vote", "campaign")
KNOWN_CONTEST_WIDE_CAMPAIGN_DIRECTORIES = {
    "https://www.cbsnews.com/minnesota/news/minneapolis-mayoral-election-candidates/",
}
KNOWN_NON_CANDIDATE_DOMAINS = {"lp.org", "themilitant.com"}

ELIGIBLE_COLUMNS = [
    "freeze_contract_version",
    "validation_method_version",
    "validation_key",
    "validation_hash",
    "lead_record_hash",
    "candidate_context_hash",
    "validation_shard_id",
    "validation_shard_count",
    "eligible_for_temporal_validation",
    "eligibility_reason",
    *URL_LEAD_COLUMNS,
]

DECISION_COLUMNS = [
    "url_claim_id",
    "url_entity_id",
    "candidate_year_id",
    "full_name",
    "exact_normalized_url",
    "mechanically_eligible",
    "candidate_token_in_host",
    "candidate_alias_in_anchor",
    "campaign_marker_in_host",
    "eligible_for_temporal_validation",
    "eligibility_reason",
]


class SearchLeadFreezeError(ValueError):
    pass


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def stable_hash(*values: object) -> str:
    return hashlib.sha256("\x1f".join(map(str, values)).encode("utf-8")).hexdigest()


def _canonical_row_hash(row: dict[str, object], columns: list[str]) -> str:
    payload = {column: _text(row.get(column, "")) for column in columns}
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def _atomic_csv(frame: pd.DataFrame, path: Path, columns: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        mode="w", encoding="utf-8", newline="", dir=path.parent, delete=False
    ) as handle:
        temporary = Path(handle.name)
        frame.reindex(columns=columns).to_csv(handle, index=False, lineterminator="\n")
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)


def _atomic_json(value: dict[str, object], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        mode="w", encoding="utf-8", newline="\n", dir=path.parent, delete=False
    ) as handle:
        temporary = Path(handle.name)
        json.dump(value, handle, indent=2, sort_keys=True)
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)


def _compact(value: object) -> str:
    return re.sub(r"[^a-z0-9]", "", _text(value).casefold())


def _alias_in_text(name: str, value: str) -> bool:
    tokens = set(re.findall(r"[a-z0-9]+", _text(value).casefold()))
    return any(set(alias).issubset(tokens) for alias in _candidate_aliases(name))


def _candidate_token_in_host(name: str, url: str) -> bool:
    host = _compact(urlsplit(url).hostname or "")
    aliases = _candidate_aliases(name)
    if not host or not aliases:
        return False
    surname = aliases[0][-1] if aliases[0] else ""
    if len(surname) >= 4 and surname in host:
        return True
    return any(
        len(alias) >= 2
        and sum(1 for token in alias if len(token) >= 3 and token in host) >= 2
        for alias in aliases
    )


def freeze_leads(
    leads: pd.DataFrame,
    page_links: pd.DataFrame,
    *,
    validation_shard_count: int = VALIDATION_SHARD_COUNT,
) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, object]]:
    if validation_shard_count <= 0:
        raise SearchLeadFreezeError("validation_shard_count must be positive")
    if leads.columns.tolist() != URL_LEAD_COLUMNS:
        raise SearchLeadFreezeError("URL lead schema differs from the current runner contract")
    if leads["url_claim_id"].duplicated().any():
        raise SearchLeadFreezeError("URL lead IDs are not unique")
    link_anchor = {}
    if len(page_links):
        if page_links["page_link_id"].duplicated().any():
            raise SearchLeadFreezeError("Page-link IDs are not unique")
        link_anchor = page_links.set_index("page_link_id")["anchor_text"].map(_text).to_dict()

    eligible_rows: list[dict[str, object]] = []
    decisions: list[dict[str, object]] = []
    for lead in leads.to_dict("records"):
        extraction_method = _text(lead["extraction_method"])
        direct = extraction_method in {
            "serper_direct_candidate_site_result",
            "openai_model_explicit_campaign_url",
        }
        mechanical = (
            _text(lead["candidate_attribution_status"]) == "identity_supported"
            and _text(lead["url_type"]) in COVERAGE_TYPES
            and _text(lead["evidence_grade"]) == "D"
            and _text(lead["review_status"]) == "pending"
            and not _text(lead["acceptance"])
            and _text(lead["strict_coverage_eligible"]).casefold() in {"false", "0", ""}
        )
        candidate_token = _candidate_token_in_host(
            _text(lead["full_name"]), _text(lead["exact_normalized_url"])
        )
        host = _compact(urlsplit(_text(lead["exact_normalized_url"])).hostname or "")
        registrable = _text(lead["registrable_domain"]).casefold()
        campaign_marker = any(marker in host for marker in CAMPAIGN_HOST_MARKERS)
        anchor = link_anchor.get(_text(lead["page_link_id"]), "")
        alias_in_anchor = _alias_in_text(_text(lead["full_name"]), anchor)

        eligible = False
        reason = ""
        if not mechanical:
            reason = "fails_identity_type_or_pending_grade_d_gate"
        elif direct:
            eligible = True
            reason = (
                "direct_serper_candidate_site_result"
                if extraction_method == "serper_direct_candidate_site_result"
                else "openai_model_explicit_campaign_url_pending_content_validation"
            )
        elif candidate_token and alias_in_anchor:
            eligible = True
            reason = "candidate_named_anchor_and_candidate_token_in_host"
        elif campaign_marker and alias_in_anchor:
            eligible = True
            reason = "candidate_named_anchor_and_campaign_marker_in_host"
        elif (
            alias_in_anchor
            and _text(lead["source_page_url"])
            in KNOWN_CONTEST_WIDE_CAMPAIGN_DIRECTORIES
            and registrable not in KNOWN_NON_CANDIDATE_DOMAINS
        ):
            eligible = True
            reason = "candidate_named_link_in_verified_contest_campaign_directory"
        elif candidate_token:
            # Link-context attribution already requires a unique full-name alias.
            eligible = True
            reason = "unique_candidate_context_and_candidate_token_in_host"
        else:
            reason = "ownership_signal_too_weak_for_archive_spend"

        decisions.append(
            {
                "url_claim_id": lead["url_claim_id"],
                "url_entity_id": lead["url_entity_id"],
                "candidate_year_id": lead["candidate_year_id"],
                "full_name": lead["full_name"],
                "exact_normalized_url": lead["exact_normalized_url"],
                "mechanically_eligible": mechanical,
                "candidate_token_in_host": candidate_token,
                "candidate_alias_in_anchor": alias_in_anchor,
                "campaign_marker_in_host": campaign_marker,
                "eligible_for_temporal_validation": eligible,
                "eligibility_reason": reason,
            }
        )
        if not eligible:
            continue
        validation_key = "\x1f".join(
            [
                _text(lead["url_claim_id"]),
                _text(lead["candidate_year_id"]),
                _text(lead["exact_normalized_url"]),
            ]
        )
        validation_hash = hashlib.sha256(validation_key.encode("utf-8")).hexdigest()
        record = {
            "freeze_contract_version": FREEZE_CONTRACT_VERSION,
            "validation_method_version": VALIDATION_METHOD_VERSION,
            "validation_key": validation_key,
            "validation_hash": validation_hash,
            "lead_record_hash": _canonical_row_hash(lead, URL_LEAD_COLUMNS),
            "candidate_context_hash": stable_hash(
                lead["candidate_year_id"],
                lead["candidate_contest_id"],
                lead["full_name"],
                lead["geo_name"],
                lead["state_abb"],
                lead["year"],
                lead["month"],
            ),
            "validation_shard_id": int(validation_hash, 16) % validation_shard_count,
            "validation_shard_count": validation_shard_count,
            "eligible_for_temporal_validation": True,
            "eligibility_reason": reason,
            **lead,
        }
        eligible_rows.append(record)

    eligible_frame = pd.DataFrame(eligible_rows).reindex(columns=ELIGIBLE_COLUMNS)
    eligible_frame = eligible_frame.sort_values(
        ["validation_hash", "validation_key"], kind="stable"
    ).reset_index(drop=True)
    decision_frame = pd.DataFrame(decisions).reindex(columns=DECISION_COLUMNS)
    decision_frame = decision_frame.sort_values("url_claim_id", kind="stable").reset_index(drop=True)
    if eligible_frame["validation_key"].duplicated().any():
        raise SearchLeadFreezeError("Temporal-validation keys are not unique")
    mechanical_n = int(decision_frame["mechanically_eligible"].sum())
    eligible_n = len(eligible_frame)
    audit = {
        "manifest_version": MANIFEST_VERSION,
        "freeze_contract_version": FREEZE_CONTRACT_VERSION,
        "validation_method_version": VALIDATION_METHOD_VERSION,
        "input_lead_rows": int(len(leads)),
        "mechanically_eligible_rows": mechanical_n,
        "eligible_rows": eligible_n,
        "eligible_entities": int(eligible_frame["url_entity_id"].nunique()),
        "eligible_candidates": int(eligible_frame["candidate_year_id"].nunique()),
        "mechanical_to_frozen_drop_rows": mechanical_n - eligible_n,
        "mechanical_to_frozen_drop_fraction": (
            (mechanical_n - eligible_n) / mechanical_n if mechanical_n else 0.0
        ),
        "large_drop_explicitly_audited": bool(
            mechanical_n and (mechanical_n - eligible_n) / mechanical_n > 0.20
        ),
        "eligibility_reason_counts": {
            str(key): int(value)
            for key, value in decision_frame["eligibility_reason"].value_counts().sort_index().items()
        },
        "validation_shard_count": validation_shard_count,
        "validation_shard_rows": {
            str(key): int(value)
            for key, value in eligible_frame["validation_shard_id"].value_counts().sort_index().items()
        },
        "strict_or_accepted_claims_created": 0,
    }
    return eligible_frame, decision_frame, audit


def build_freeze(
    leads_path: Path = DEFAULT_LEADS,
    links_path: Path = DEFAULT_LINKS,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    validation_shard_count: int = VALIDATION_SHARD_COUNT,
) -> dict[str, object]:
    leads_path = leads_path.resolve()
    links_path = links_path.resolve()
    output_dir = output_dir.resolve()
    leads = pd.read_csv(leads_path, keep_default_na=False, low_memory=False)
    links = pd.read_csv(links_path, keep_default_na=False, low_memory=False)
    eligible, decisions, audit = freeze_leads(
        leads, links, validation_shard_count=validation_shard_count
    )
    eligible_path = output_dir / "eligible_leads.csv"
    decisions_path = output_dir / "lead_filter_decisions.csv"
    _atomic_csv(eligible, eligible_path, ELIGIBLE_COLUMNS)
    _atomic_csv(decisions, decisions_path, DECISION_COLUMNS)
    audit.update(
        {
            "input_url_leads": {
                "path": str(leads_path.relative_to(REPO_ROOT)),
                "rows": int(len(leads)),
                "sha256": sha256_file(leads_path),
            },
            "input_page_links": {
                "path": str(links_path.relative_to(REPO_ROOT)),
                "rows": int(len(links)),
                "sha256": sha256_file(links_path),
            },
            "eligible_output": {
                "path": str(eligible_path.relative_to(REPO_ROOT)),
                "rows": int(len(eligible)),
                "bytes": eligible_path.stat().st_size,
                "sha256": sha256_file(eligible_path),
                "columns": ELIGIBLE_COLUMNS,
            },
            "decision_output": {
                "path": str(decisions_path.relative_to(REPO_ROOT)),
                "rows": int(len(decisions)),
                "bytes": decisions_path.stat().st_size,
                "sha256": sha256_file(decisions_path),
            },
            "key_definition": "url_claim_id + U+001F + candidate_year_id + U+001F + exact_normalized_url",
            "coverage_url_types": sorted(COVERAGE_TYPES),
            "verified_contest_campaign_directories": sorted(
                KNOWN_CONTEST_WIDE_CAMPAIGN_DIRECTORIES
            ),
            "known_non_candidate_domains": sorted(KNOWN_NON_CANDIDATE_DOMAINS),
            "required_identity_status": "identity_supported",
        }
    )
    _atomic_json(audit, output_dir / "manifest.json")
    return audit


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--leads", type=Path, default=DEFAULT_LEADS)
    parser.add_argument("--links", type=Path, default=DEFAULT_LINKS)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--validation-shard-count", type=int, default=VALIDATION_SHARD_COUNT)
    args = parser.parse_args()
    print(
        json.dumps(
            build_freeze(
                args.leads,
                args.links,
                args.output_dir,
                args.validation_shard_count,
            ),
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
