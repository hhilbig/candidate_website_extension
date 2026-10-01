#!/usr/bin/env python3
"""Scan unused public Archive-It election collections for mayoral URL leads."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import tempfile
import unicodedata
from pathlib import Path
from typing import Any, Mapping
from urllib.parse import urlsplit

import pandas as pd

from scripts.discover_archiveit_mayoral_urls import (
    CollectionConfig,
    _payload_records,
    candidate_name_hints,
    classify_url,
    fetch_partner_seed_json,
    normalize_exact_url,
    registrable_domain,
    site_root,
    unwrap_archive_url,
)


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_UNIVERSE = (
    REPO_ROOT / "data/municipal_discovery/screening_2010_2021/"
    "full_discovery_2010_2021/production_universe.csv"
)
DEFAULT_OUTPUT_DIR = (
    REPO_ROOT / "data/municipal_discovery/archiveit/cross_collection_2010_2021"
)
PARSER_VERSION = "archive_it_cross_collection_exact_identity_v1"

# These are unused U.S. election/candidate collections with plausible local
# content. Foreign municipal collections and known office-specific federal
# collections are deliberately excluded.
TARGETS = {
    11325: ("Black Politicians Elections 2018 - Present", 2018, 2021),
    7220: ("2016 General Election in Arizona", 2016, 2019),
    10605: ("2018 New York Elections", 2018, 2018),
    10793: ("State Elections", 2018, 2021),
    12955: ("San Mateo and Santa Clara 2019 Election", 2019, 2019),
    14820: ("SWMN Election 2020", 2020, 2020),
    15342: ("US Election Nov. 3, 2020", 2020, 2020),
}

ATTEMPT_COLUMNS = [
    "collection_id", "collection_name", "source_status", "seed_rows",
    "exact_identity_rows", "eligible_leads", "error", "parser_version",
]
AUDIT_COLUMNS = [
    "collection_id", "seed_id", "raw_url", "exact_normalized_url",
    "candidate_name_hints", "exact_candidate_ids_json", "disposition",
    "disposition_reason", "source_seed_json", "parser_version",
]
LEAD_COLUMNS = [
    "claim_id", "source_id", "candidate_year_id", "candidate_contest_id",
    "contest", "full_name", "geo_name", "state_abb", "year", "month",
    "collection_id", "collection_name", "seed_id", "raw_url",
    "exact_normalized_url", "site_root_url", "registrable_domain", "url_type",
    "identity_match", "temporal_match", "evidence_grade", "review_status",
    "acceptance", "source_profile_url", "evidence_text", "parser_version",
]

EXCLUDED_HOSTS = {
    "facebook.com", "twitter.com", "x.com", "instagram.com", "youtube.com",
    "linkedin.com", "actblue.com", "winred.com", "ballotpedia.org",
    "wikipedia.org",
}
OTHER_OFFICE_TERMS = re.compile(
    r"\b(?:congress|senate|senator|representative|governor|school\s*board|"
    r"county\s*commission|state\s*house|state\s*assembly)\b", re.I
)


def _norm(value: object) -> str:
    text = unicodedata.normalize("NFKD", str(value or ""))
    text = "".join(ch for ch in text if not unicodedata.combining(ch)).casefold()
    return " ".join(re.findall(r"[a-z0-9]+", text))


def _metadata_text(seed: Mapping[str, Any]) -> str:
    metadata = seed.get("metadata") or {}
    values: list[str] = []
    if isinstance(metadata, Mapping):
        for value in metadata.values():
            if isinstance(value, list):
                values.extend(str(item) for item in value)
            else:
                values.append(str(value))
    return " ".join(values)


def _atomic_csv(frame: pd.DataFrame, path: Path, columns: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        mode="w", encoding="utf-8", newline="", dir=path.parent, delete=False
    ) as handle:
        temporary = Path(handle.name)
        frame.reindex(columns=columns).to_csv(handle, index=False, lineterminator="\n")
    temporary.replace(path)


def scan_collection(
    collection_id: int,
    payload: object,
    universe: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    name, start_year, end_year = TARGETS[collection_id]
    candidates = universe.loc[
        pd.to_numeric(universe["year"], errors="coerce").between(start_year, end_year)
    ].copy()
    candidates["_name"] = candidates["full_name"].map(_norm)
    name_groups = {
        key: group for key, group in candidates.groupby("_name", sort=False) if key
    }
    config = CollectionConfig(
        collection_id, f"archive_it_collection_{collection_id}", name,
        "cross_collection", "US", start_year, end_year,
        collection_scoped_mayoral_seeds=True,
    )
    audits: list[dict[str, object]] = []
    leads: list[dict[str, object]] = []
    for seed in _payload_records(payload):
        raw_url = str(seed.get("url") or seed.get("canonical_url") or "").strip()
        exact_url = normalize_exact_url(unwrap_archive_url(raw_url))
        hints = candidate_name_hints(seed, config)
        matched = pd.concat(
            [name_groups[_norm(hint)] for hint in hints if _norm(hint) in name_groups],
            ignore_index=True,
        ).drop_duplicates("candidate_year_id") if any(
            _norm(hint) in name_groups for hint in hints
        ) else candidates.iloc[0:0]
        context = " ".join([raw_url, _metadata_text(seed)])
        host = (urlsplit(exact_url).hostname or "").casefold().removeprefix("www.")
        disposition = "no_exact_candidate_identity"
        reason = "no metadata hint exactly equals an in-period candidate name"
        eligible = False
        candidate: pd.Series | None = None
        if len(matched) > 1:
            disposition = "match_unresolved"
            reason = "exact name maps to multiple candidate-years"
        elif len(matched) == 1:
            candidate = matched.iloc[0]
            city_signal = _norm(candidate["geo_name"]) in _norm(context)
            mayor_signal = bool(re.search(r"\bmayor(?:al)?\b", context, re.I))
            excluded_host = any(host == item or host.endswith(f".{item}") for item in EXCLUDED_HOSTS)
            if excluded_host:
                disposition, reason = "excluded_url_type", "social/profile/fundraising host"
            elif OTHER_OFFICE_TERMS.search(context):
                disposition, reason = "other_office_conflict", "source context names a non-mayoral office"
            elif not (city_signal or mayor_signal):
                disposition, reason = "mayoral_context_unresolved", "neither city nor mayor appears in URL/source context"
            elif not exact_url:
                disposition, reason = "invalid_url", "seed has no HTTP(S) target"
            else:
                disposition, reason, eligible = (
                    "eligible_pending_review",
                    "exact metadata name plus candidate city or mayoral signal",
                    True,
                )
        matched_ids = sorted(matched["candidate_year_id"].astype(str).unique())
        audits.append({
            "collection_id": collection_id, "seed_id": str(seed.get("id", "")),
            "raw_url": raw_url, "exact_normalized_url": exact_url,
            "candidate_name_hints": " | ".join(hints),
            "exact_candidate_ids_json": json.dumps(matched_ids, separators=(",", ":")),
            "disposition": disposition, "disposition_reason": reason,
            "source_seed_json": json.dumps(seed, ensure_ascii=False, sort_keys=True, separators=(",", ":")),
            "parser_version": PARSER_VERSION,
        })
        if eligible and candidate is not None:
            claim_id = "archiveit_cross_claim_" + hashlib.sha256(
                f"{collection_id}\x1f{seed.get('id','')}\x1f{candidate['candidate_year_id']}\x1f{exact_url}".encode()
            ).hexdigest()[:24]
            leads.append({
                "claim_id": claim_id, "source_id": f"archive_it_collection_{collection_id}",
                "candidate_year_id": candidate["candidate_year_id"],
                "candidate_contest_id": candidate["candidate_contest_id"],
                "contest": candidate["contest"], "full_name": candidate["full_name"],
                "geo_name": candidate["geo_name"], "state_abb": candidate["state_abb"],
                "year": candidate["year"], "month": candidate["month"],
                "collection_id": collection_id, "collection_name": name,
                "seed_id": str(seed.get("id", "")), "raw_url": raw_url,
                "exact_normalized_url": exact_url, "site_root_url": site_root(exact_url),
                "registrable_domain": registrable_domain(exact_url),
                "url_type": classify_url(exact_url, seed.get("metadata") or {}),
                "identity_match": True, "temporal_match": "collection_period_only",
                "evidence_grade": "D", "review_status": "pending", "acceptance": "",
                "source_profile_url": f"https://archive-it.org/collections/{collection_id}",
                "evidence_text": reason, "parser_version": PARSER_VERSION,
            })
    return (
        pd.DataFrame(audits).reindex(columns=AUDIT_COLUMNS),
        pd.DataFrame(leads).reindex(columns=LEAD_COLUMNS),
    )


def run(universe_path: Path = DEFAULT_UNIVERSE, output_dir: Path = DEFAULT_OUTPUT_DIR) -> dict[str, object]:
    universe = pd.read_csv(universe_path, low_memory=False)
    attempts: list[dict[str, object]] = []
    audits: list[pd.DataFrame] = []
    leads: list[pd.DataFrame] = []
    payload_dir = output_dir / "source_payloads"
    payload_dir.mkdir(parents=True, exist_ok=True)
    for collection_id, (name, _, _) in TARGETS.items():
        try:
            payload = fetch_partner_seed_json(collection_id, timeout=30, attempts=2)
            payload_path = payload_dir / f"{collection_id}.json"
            payload_path.write_text(
                json.dumps(payload, ensure_ascii=False, sort_keys=True) + "\n",
                encoding="utf-8",
            )
            seed_audit, collection_leads = scan_collection(collection_id, payload, universe)
            status = "found" if len(collection_leads) else "no_result"
            attempts.append({
                "collection_id": collection_id, "collection_name": name,
                "source_status": status, "seed_rows": len(seed_audit),
                "exact_identity_rows": int(seed_audit.exact_candidate_ids_json.ne("[]").sum()),
                "eligible_leads": len(collection_leads), "error": "",
                "parser_version": PARSER_VERSION,
            })
            audits.append(seed_audit)
            leads.append(collection_leads)
        except Exception as exc:  # terminal source ledger must retain every target
            attempts.append({
                "collection_id": collection_id, "collection_name": name,
                "source_status": "request_error", "seed_rows": 0,
                "exact_identity_rows": 0, "eligible_leads": 0,
                "error": f"{type(exc).__name__}: {exc}"[:1000],
                "parser_version": PARSER_VERSION,
            })
    attempt_frame = pd.DataFrame(attempts).reindex(columns=ATTEMPT_COLUMNS)
    audit_frame = pd.concat(audits, ignore_index=True) if audits else pd.DataFrame(columns=AUDIT_COLUMNS)
    lead_frame = pd.concat(leads, ignore_index=True) if leads else pd.DataFrame(columns=LEAD_COLUMNS)
    if attempt_frame.collection_id.nunique() != len(TARGETS):
        raise AssertionError("Archive-It attempt ledger lacks an exact target key")
    if not lead_frame.empty and lead_frame.claim_id.duplicated().any():
        raise AssertionError("Archive-It cross-collection claim IDs are not unique")
    _atomic_csv(attempt_frame, output_dir / "attempts.csv", ATTEMPT_COLUMNS)
    _atomic_csv(audit_frame, output_dir / "seed_audit.csv", AUDIT_COLUMNS)
    _atomic_csv(lead_frame, output_dir / "url_leads.csv", LEAD_COLUMNS)
    summary = {
        "parser_version": PARSER_VERSION, "collections_targeted": len(TARGETS),
        "collection_statuses": attempt_frame.source_status.value_counts().sort_index().astype(int).to_dict(),
        "seed_rows": len(audit_frame), "eligible_leads": len(lead_frame),
        "candidate_years": int(lead_frame.candidate_year_id.nunique()) if len(lead_frame) else 0,
        "seed_to_lead_drop_rate": float(1 - len(lead_frame) / len(audit_frame)) if len(audit_frame) else 0,
        "large_drop_explanation": "Expected high-precision filter: most election-collection seeds concern other offices or lack exact candidate identity plus mayor/city context.",
        "strict_urls_added": 0,
    }
    (output_dir / "audit.json").write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n")
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--universe", type=Path, default=DEFAULT_UNIVERSE)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    args = parser.parse_args()
    print(json.dumps(run(args.universe, args.output_dir), indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
